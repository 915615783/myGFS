import network
import socket
import os
import shutil
import argparse
import threading
import time

def thread_wraper(f):
    def w_f(self, sock, request):
        key = request.get('key')
        assert key != None
        user_name = key.split('_')[0]
        t = threading.Thread(target=f, args=(self, sock, request))
        t.setDaemon(True)
        self.user2thread[user_name] = t
        print('new thread serving user %s.'%user_name)
        t.start()
    return w_f

def sock_close_wraper(f):
    def w_f(self, sock, request):
        try:
            f(self, sock, request)
        except Exception as e:
            print(e, 'Error happens in functioin', f.__name__)
    return w_f

class Chunck():
    def __init__(self, address, buffer_dir, save_dir, master_address):
        self.address = address
        self.buffer_dir = buffer_dir
        self.save_dir = save_dir
        if os.path.isdir(buffer_dir):
            shutil.rmtree(buffer_dir)
        os.mkdir(buffer_dir)
        if os.path.isdir(save_dir):
            shutil.rmtree(save_dir)
        os.mkdir(save_dir)
        
        self.key2info = {}
        self.user2thread = {}

        self.master_address = master_address
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('0.0.0.0', self.address[1]))
        self.sock.listen()

        # heart beat用来确认服务器没有挂掉，也用来删除master没有了，但是chunck还保存着的文件
        # 如果从缓冲区复制到存储区，到客户端想master报到前，发生了heart beat，很可能会把刚上传的文件给删了
        # 因此采取一个策略：把多次heart beat master返回的文件信息存起来，检查如果多次heart beat都不存在的文件，才把它删掉
        self.delete_check_time = 2  # 多少次
        self.heart_beat_time = 10

    def heart_beat(self):
        print('Heart beat thread started.')
        while True:
            try:
                time.sleep(self.heart_beat_time)
                print('** Heart Beat **')
                request = {'command': 'heart beat', 'address': self.address}
                response = self.ask_master(request)
                assert isinstance(response.get('response'), dict)
                for my_key in self.key2info.keys():
                    if my_key not in response.get('response'):
                        if self.key2info[my_key].get('not exist time') == None:
                            self.key2info[my_key]['not exist time'] = 1
                        else:
                            self.key2info[my_key]['not exist time'] += 1


                # print('===========', self.key2info)
                pop_key = []
                for key in self.key2info.keys():
                    if self.key2info[key].get('not exist time') != None:
                        if self.key2info[key].get('not exist time') > self.delete_check_time:
                            print('**** 文件%s将被删除，因为master中已经没有它的记录 ****'%key)
                            for i in range(self.key2info.get(key).get('num_blocks')):
                                saved_path = self.save_dir + key + '_' + str(i) + '.block'
                                if os.path.isfile(saved_path):
                                    os.remove(saved_path)
                            pop_key.append(key)
                for i in pop_key:
                    self.key2info.pop(i)
            except Exception as e:
                print('heart beat thread:', str(e))
                # raise e

    
    def run_server(self):
        print('Chunck server begins running.', ('0.0.0.0', self.address[1]))
        if not self.join_cluster():
            print('Can not join cluster.')
            return False
        print('Successfully join cluster.')
        print('Begin listening...')

        # 创建心跳线程
        hbt = threading.Thread(target=self.heart_beat)
        hbt.setDaemon(True)
        hbt.start()

        while True:
            try:
                req_sock, req_address = self.sock.accept()
                request = network.recv_dict(req_sock, timeout=20)
                
                # 检查当前用户是否有活着的线程
                key = request.get('key')
                assert key != None
                user_name = key.split('_')[0]
                if self.user2thread.get(user_name) != None:
                    if self.user2thread.get(user_name).isAlive():
                        raise Exception('用户%s已经有线程在运行，又来一个请求我只不客气了，呵呵'%user_name)

                print(req_address, request)
                if request.get('command') == 'get':
                    self.get(req_sock, request)
                elif request.get('command') == 'push to buffer':
                    self.push(req_sock, request)
                elif request.get('command') == 'delete':
                    self.delete(req_sock, request)
                else:
                    # response = {'response':'unvalid_command'}
                    raise Exception('unvalid command')

            except Exception as e:
                print(e, req_address)
                req_sock.close()

    @thread_wraper
    @sock_close_wraper
    def delete(self, sock, request):
        print('deleteing')
        key = request.get('key')
        if key in self.key2info:
            for i in range(self.key2info.get(key).get('num_blocks')):
                saved_path = self.save_dir + key + '_' + str(i) + '.block'
                if os.path.isfile(saved_path):
                    os.remove(saved_path)
            self.key2info.pop(key)
        network.send_dict(sock, {'response': 'deleted'})

    @thread_wraper
    @sock_close_wraper
    def push(self, sock, request):
        key = request.get('key')
        network.send_dict(sock, {'response':'ready'})
        num_blocks = network.recv_from_blocks_to_blocks(sock, self.buffer_dir + key)

        while True:
            request = network.recv_dict(sock)
            if request.get('command') != 'wait':
                break
            print('waiting')
        
        assert request.get('command') == 'push confirm'
        # 判断是否已经存在, 有则删除
        if key in self.key2info:
            for i in range(self.key2info.get(key).get('num_blocks')):
                saved_path = self.save_dir + key + '_' + str(i) + '.block'
                if os.path.isfile(saved_path):
                    os.remove(saved_path)
            self.key2info.pop(key)
        # 复制过去
        for i in range(num_blocks):
            buffer_path = self.buffer_dir + key + '_' + str(i) + '.block'
            target_path = self.save_dir + key + '_' + str(i) + '.block'
            shutil.move(buffer_path, target_path)
        self.key2info[key] = {'num_blocks': num_blocks} 
        
        network.send_dict(sock, {'response': 'push success', 'num_blocks': num_blocks})
        

        
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


    @thread_wraper
    @sock_close_wraper
    def get(self, sock, request_dict):
        key = request_dict['key']
        num_blocks = request_dict['num_blocks']
        network.send_from_blocks_to_blocks(sock, self.save_dir+key, num_blocks)

    def join_cluster(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(self.master_address)
            request = {'command': 'join cluster', 'address':self.address}
            network.send_dict(sock, request)
            response = network.recv_dict(sock)
            sock.close()
            if response['response'] != 'success':
                return False
            return True
        except socket.timeout:
            sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', type=str, default='127.0.0.1', help='Chunck will listen this ip')
    parser.add_argument('--port', type=int, default=14488, help='Chunck will listen this tcp port')
    parser.add_argument('--master_ip', type=str, default='127.0.0.1', help='master ip')
    parser.add_argument('--master_port', type=int, default=14477, help='master port')
    opt = parser.parse_args()
    address = (opt.ip, opt.port)
    master_address = (opt.master_ip, opt.master_port)
    chunck = Chunck(address, 'buffer_dir/', 'save_dir/', master_address)
    chunck.run_server()

if __name__ == '__main__':
    main()