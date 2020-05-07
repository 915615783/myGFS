import socket
import network
import os
import tkinter as tk
import time
import threading
import windnd
import tkinter.messagebox as messagebox
from network import BLOCKSIZE

class Client():
    def __init__(self, my_name, master_address):
        self.my_name = my_name
        self.master_address = master_address
        self.save_dir = 'client_download/'
        if not os.path.isdir(self.save_dir):
            os.mkdir(self.save_dir)

    def run_gui(self):
        self.main_window = tk.Tk()
        self.main_window.title('GGFS(%s)'%(self.my_name))
        self.main_window.geometry('300x400')
        self.state_var = tk.StringVar()
        self.state_var.set('你好, %s'%(self.my_name))
        self.state_label = tk.Label(self.main_window, textvariable=self.state_var)
        self.state_label.pack()
        
        tk.Label(self.main_window, text='你的云文件:').place(x=3, y=43, height=18)
        self.file_listbox = tk.Listbox(self.main_window)
        self.refresh_file_listbox()
        self.file_listbox.place(x=5, y=60, width=290, height=335)
        self.file_listbox.bind('<Double-Button-1>', self.double_click_download)
        self.file_listbox.bind('<Double-Button-3>', self.right_double_click_download)
        # 绑定拖拽时间到上传功能
        windnd.hook_dropfiles(self.main_window, func=self.dropfile_push)
        
        self.main_window.mainloop()

    def dropfile_push(self, files):
        if len(files) != 1:
            self.state_var.set('一次只能拖一个文件，谢谢')
            return
        file_path = files[0].decode('gbk')
        key = os.path.split(file_path)[1]
        print(file_path, '----上传---->', key)
        t = threading.Thread(target=self.push, args=(key, file_path))
        t.setDaemon(True)
        t.start()
    

    def refresh_file_listbox(self):
        self.file_listbox.delete(0, tk.END)
        response = self.get_file_list()
        self.file_list_without_username = ['_'.join(i.split('_')[1:]) for i in response['response']]
        for file_name in self.file_list_without_username:
            self.file_listbox.insert(tk.END, file_name)

    def double_click_download(self, event):
        file_name = self.file_list_without_username[self.file_listbox.curselection()[0]]
        save_path = self.save_dir + file_name
        t = threading.Thread(target=self.get, args=(file_name, save_path, self.state_var))
        t.setDaemon(True)
        t.start()
        # self.get(file_name, save_path, state_var=self.state_var)

    def right_double_click_download(self, event):
        # 删除文件
        try:
            file_name = self.file_list_without_username[self.file_listbox.curselection()[0]]
            t = threading.Thread(target=self.delete, args=(file_name,))
            t.setDaemon(True)
            t.start()
        except Exception as e:
            print(e)

        
    def get_file_list(self):
        request = {'command':'get file list', 'user_name': self.my_name}
        return self.ask_master(request)

    def delete(self, key):
        self.state_var.set('Deleting %s in master'%key)
        time.sleep(1.2)
        key = self.my_name + '_' + key
        request = {'command': 'delete', 'key': key}
        response = self.ask_master(request)
        assert response.get('response') == 'deleted'

        self.state_var.set('Master deleted!')
        time.sleep(1.2)

        self.state_var.set('deleting chunck...')
        time.sleep(1.2)

        if response.get('addresses') != None:
            for i in response.get('addresses'):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect(tuple(i))
                    network.send_dict(sock, {'command':'delete', 'key':key})
                    c_response = network.recv_dict(sock)
                    assert c_response['response'] == 'deleted'
                except Exception as e:
                    self.state_var.set(str(e))
                    # sock.close()
                    # raise e
                sock.close()

        self.state_var.set('deleted!')
        self.refresh_file_listbox()



    def push(self, key, file_path):
        chunck_sock = []
        try:
            # 先向master询问，向哪几台服务器写。如果已经有2台服务器有，则会多返回一台
            self.state_var.set('Begin pushing file and asking master')
            time.sleep(1.2)
            key = self.my_name + '_' + key
            request = {'command': 'push', 'key': key}
            response = self.ask_master(request)
            if response.get('response') == 'no chunck':
                self.state_var.set('No chunck!')
                return
            chunck_addresses = response['response']
            is_cover = response['cover']

            # 多线程向chunck缓冲区发送数据
            self.state_var.set('uploading to chunck buffer')
            print('往这些chunck buffer上传文件中:', chunck_addresses)
            time.sleep(1.2)
            chunck_sock = []
            send_thread = []
            finish_flag = [False] * len(chunck_addresses)
            finish_flag_lock = threading.Lock()
            self.push_total_blocks = int((os.path.getsize(file_path) * len(chunck_addresses))) / BLOCKSIZE + len(chunck_addresses)
            self.push_finish_total_blocks = 0
            self.push_total_blocks_lock = threading.Lock()
            for i in chunck_addresses:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(tuple(i))
                chunck_sock.append(sock)
            for index, sc in enumerate(chunck_sock):
                t = threading.Thread(target=self.send_to_chunck_buffer, 
                    args=(sc, key, file_path, finish_flag, index, finish_flag_lock))
                t.setDaemon(True)
                send_thread.append(t)
                t.start()
            
            t_wait = threading.Thread(target=self.wait_for_all_sending_thread, 
                args=(chunck_sock, finish_flag, finish_flag_lock, send_thread, chunck_addresses))
            t_wait.setDaemon(True)
            t_wait.start()

            for i in send_thread:
                i.join()
            t_wait.join()

            if False in finish_flag:
                raise Exception('Some chuncks do not recieve successfully.')

            self.state_var.set('Successfully upload!')
            time.sleep(1.2)

            # 如果有重名，上传完缓冲区之后，向master确定，master把重名的记录删除。
            # 也就是说，如果上传缓冲区不成功，之前的记录是不会被去掉。但是成功之后，无论后面写入是否成功，之前的都没了
            if is_cover:
                self.state_var.set('covering! deleting master record')
                time.sleep(1.2)
                request = {'command':'delete', 'key': key}
                response = self.ask_master(request)
                assert response.get('response') == 'deleted'

            # 等待发送完(如果有不行的，直接断开连接，chunck收到异常会清空缓冲区)，再发送确认写入命令
            # 等待写入成功回复（只要有成功写入就行，都不成功就断开吧。这个应该都会成功的，毕竟都发过去了）
            self.state_var.set('Confirming push with chunck...')
            time.sleep(1.2)
            confirm_thread = []
            confirm_return = []
            for i in chunck_sock:
                confirm_return.append({})
                t = threading.Thread(target=self.push_confirm, args=(i, confirm_return[-1]))
                t.setDaemon(True)
                t.start()
                confirm_thread.append(t)

            for i in confirm_thread:
                i.join()
            num_blocks = confirm_return[0].get('num_blocks')
            for i in confirm_return:
                if i.get('Error') != None:
                    raise i.get('Error')
                if num_blocks != i.get('num_blocks'):
                    raise Exception('各个chunck的num_blocks不同')

            self.state_var.set('Confirmed.')
            time.sleep(1.2)
                
            
            # 和master确认（如果不是全部写入成功，就把部分成功的告诉master，失败的也告诉，master后续处理。）
            # 等待master回复成功
            self.state_var.set('Confirmed push with master.')
            time.sleep(1.2)

            request = {'command': 'push finish', 'key': key,
                'info':{'addresses': chunck_addresses, 'num_blocks': num_blocks}}
            response = self.ask_master(request)
            assert response.get('response') == 'push finish success'

            self.state_var.set('Successfully pushed!')
            time.sleep(1.2)
        except Exception as e:
            for i in chunck_sock:
                i.close()
            self.state_var.set(str(e))
            self.refresh_file_listbox()
            raise e

        for i in chunck_sock:
            i.close()
        self.refresh_file_listbox()

    def push_confirm(self, chunck_sock, return_dict):
        try:
            request = {'command': 'push confirm'}
            network.send_dict(chunck_sock, request)
            response = network.recv_dict(chunck_sock)
            assert response.get('response') == 'push success'
            assert response.get('num_blocks') != None
            return_dict['response'] = 'push success'
            return_dict['num_blocks'] = response.get('num_blocks')
        except Exception as e:
            return_dict['Error'] = e


    def wait_for_all_sending_thread(self, sock_list, finish_flag, finish_flag_lock, thread_list, addresses_list):
        while True:
            is_all_dead = True
            for i in thread_list:
                if i.isAlive() == True:
                    is_all_dead = False
                    break
            if is_all_dead:
                break

            for i in range(len(sock_list)):
                finish_flag_lock.acquire()
                if finish_flag[i] == True:
                    network.send_dict(sock_list[i], {'command': 'wait'})
                    print('tell it to wait', addresses_list[i])
                finish_flag_lock.release()
            
            time.sleep(7)



    def send_to_chunck_buffer(self, sock, key, file_path, finish_flag, index, finish_flag_lock):
        request = {'command': 'push to buffer', 'key': key}
        network.send_dict(sock, request)
        response = network.recv_dict(sock)
        if response.get('response') == 'ready':
            network.send_from_full_to_blocks(sock, file_path, client=self)
        finish_flag_lock.acquire()
        finish_flag[index] = True
        finish_flag_lock.release()
    
    def get(self, key, save_path, state_var):
        '''
        key: file name without user name
        get file from cloud. if no such file, return None
        key: (str) file name
        '''
        state_var.set('Begin getting file and asking master:' + key)
        time.sleep(1.2)
        # state_var.set(str(123))
        key = self.my_name + '_' + key
        # 第一步，询问master存储的地址
        request = {'command': 'get', 'key': key}
        try:
            response = self.ask_master(request)
        except Exception as e:
            state_var.set(str(e))
        if response['response'] == 'no_such_file':
            print('Get failed because no such file.')
            return None
        # 第二步，向chunck取文件
        for i, chunck_add in enumerate(response['response']['addresses']):
            state_var.set('Begin getting file from '+str(chunck_add))
            time.sleep(1.2)
            try:
                self.get_from_chunck(chunck_add, key, save_path, response['response']['num_blocks'], state_var=state_var)
                state_var.set(('Successfully get file %s \nfrom chunck server '%(key)) + str(chunck_add))
            except socket.timeout as e:
                if os.path.isfile(save_path):
                    os.remove(save_path)
                state_var.set('timeout')
                if i == len(response['response']['addresses']):
                    return None
                continue
            except Exception as e:
                state_var.set(str(e))
                if os.path.isfile(save_path):
                    os.remove(save_path)
                if i == len(response['response']['addresses']):
                    return None
                continue
            return True


    def get_from_chunck(self, chunck_address, key, save_path, num_blocks, state_var=None, timeout=20):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(tuple(chunck_address))
        request = {'command': 'get', 'key': key, 'num_blocks':num_blocks}
        network.send_dict(sock, request)
        network.recv_from_blocks_to_full(sock, save_path, num_blocks, state_var=state_var)
        sock.close()
        return True




        

    def ask_master(self, request_dict):
        '''
        request_dict: {'command':'xxx', ...}
        '''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.master_address)
        network.send_dict(sock, request_dict)
        response = network.recv_dict(sock)
        sock.close()
        return response

def login_window():
    login = tk.Tk()
    login.title('登陆GGFS')
    login.geometry('300x200')
    label = tk.Label(login, text='请输入用户名(还没账户的话直接输入就是注册)')
    label.pack()
    en = tk.Entry(login)
    en.pack()

    label = tk.Label(login, text='master ip')
    label.pack()
    ip_en = tk.Entry(login)

    ip_en.pack()

    label = tk.Label(login, text='master port')
    label.pack()
    port_en = tk.Entry(login)
    port_en.insert(0, '14477')
    port_en.pack()

    login_user_name = []
    master_address = []
    def button_login():
        text = en.get().strip()
        ip = ip_en.get().strip()
        port = int(port_en.get().strip())
        login.destroy()

        login_user_name.append(text)
        master_address.append((ip, port))

        

    bu = tk.Button(login, text='登陆', command=button_login)
    bu.pack()
    login.mainloop()
    return login_user_name[0], master_address[0]

def main():
    my_name, master_address = login_window()
    try:
        client = Client(my_name, master_address)
        client.run_gui()
    except Exception as e:
        messagebox.showerror(title='Error', message=str(e))
        time.sleep(1.5)
        exit()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error in main:', e)