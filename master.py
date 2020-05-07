import socket
import network
import random
import argparse
import threading
import time

class Master():
    def __init__(self, address, m=3):
        self.address = address
        self.key2info = {}
        self.chunck_servers = []
        self.chunck_last_heart_beat_time = {}
        self.max_heart_beat_time = 60
        self.heart_beat_lock = threading.Lock()
        self.check_expire_time = 20
        self.m = m
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(address)
        self.sock.listen()

    def run_server(self):
        print('Master server begins listening...', self.address)

        # start check expire thread
        cet = threading.Thread(target=self.check_expire)
        cet.setDaemon(True)
        cet.start()

        while True:
            try:
                req_sock, req_address = self.sock.accept()
                request = network.recv_dict(req_sock, timeout=20)
                print('Get request:', req_address, request)
                if request.get('command') == 'get':
                    response = self.get(request)
                    network.send_dict(req_sock, response)
                elif request.get('command') == 'join cluster':
                    self.join_cluster(req_sock, request)
                elif request.get('command') == 'get file list':
                    self.get_file_list(req_sock, request)
                elif request.get('command') == 'push':
                    self.push(req_sock, request)
                elif request.get('command') == 'delete':
                    self.delete(req_sock, request)
                elif request.get('command') == 'push finish':
                    self.push_finish(req_sock, request)
                elif request.get('command') == 'heart beat':
                    self.heart_beat(req_sock, request)
                else:
                    response = {'response':'unvalid_command'}
            except Exception as e:
                print(e)
            finally:
                req_sock.close()

    def check_expire(self):
        print('Check expire thread started.')
        while True:
            time.sleep(self.check_expire_time)
            self.heart_beat_lock.acquire()
            try:
                print('**  Check Expire  **')
                current_time = time.time()
                expire_chuncks = []
                for chunck, hbtime in self.chunck_last_heart_beat_time.items():
                    if (current_time - hbtime) > self.max_heart_beat_time:
                        print('****', chunck, 'is expire and will be removed. ****')
                        expire_chuncks.append(chunck)
                        # self.key2info
                        pop_key = []
                        for k, i in self.key2info.items():
                            if chunck in i.get('addresses'):
                                i.get('addresses').remove(chunck)
                                if len(i.get('addresses')) == 0:
                                    pop_key.append(k)
                        for i in pop_key:
                            self.key2info.pop(i)
                        # self.chunck_servers
                        self.chunck_servers.remove(chunck)
                # self.chunck_last_heart_beat_time
                for i in expire_chuncks:
                    self.chunck_last_heart_beat_time.pop(i)
                if len(expire_chuncks) != 0:
                    print('chunck servers', self.chunck_servers)
            except Exception as e:
                print('check expire thread error', e)
            finally:
                self.heart_beat_lock.release()


    def heart_beat(self, sock, request):
        address = request.get('address')
        address = tuple(address)

        self.heart_beat_lock.acquire()
        try:
            if address not in self.chunck_servers:
                self.chunck_servers.append(address)
                print(address, '被踢出集群后收到它的心跳包，它的文件将被清楚')
            self.chunck_last_heart_beat_time[address] = time.time()
            chunck_key2info = {}
            for key, info in self.key2info.items():
                if address in info.get('addresses'):
                    chunck_key2info[key] = {'num_blocks': info.get('num_blocks')}
            response = {'response': chunck_key2info}
        except Exception as e:
            self.heart_beat_lock.release()
            raise e
        self.heart_beat_lock.release()
        network.send_dict(sock, response)

    def push_finish(self, sock, request):
        key = request.get('key')
        if key == None:
            return
        self.key2info[key] = request.get('info')
        self.key2info[key]['addresses'] = [tuple(i) for i in self.key2info[key]['addresses']]
        network.send_dict(sock, {'response': 'push finish success'})

    def delete(self, sock, request):
        response ={'response': 'deleted'}
        if self.key2info.get(request.get('key')) != None:
            chunck_addresses = self.key2info.get(request.get('key')).get('addresses')
            response['addresses'] = chunck_addresses
            self.key2info.pop(request.get('key'))
        network.send_dict(sock, response)

    def push(self, sock, request):
        '''response中cover为True表示有重名的文件，需要覆盖'''
        if len(self.chunck_servers) == 0:
            network.send_dict(sock, {'response':'no chunck'})
            return

        key = request.get('key')
        saved_info = self.key2info.get(key)
        if saved_info == None:
            saved_num = 0
        else:
            saved_num = len(saved_info.get('addresses'))

        require_num = min(self.m, len(self.chunck_servers))
        if saved_num >= require_num:
            response = {'response': saved_info.get('addresses'), 'cover':True}
        elif saved_num == 0:
            response = {'response': random.sample(self.chunck_servers, require_num), 'cover':False}
        else:
            candidate_chunck = [i for i in self.chunck_servers if i not in saved_info.get('addresses')]
            response = {'response':
                random.sample(candidate_chunck, require_num-saved_num) + saved_info.get('addresses'), 
                'cover': True}
        network.send_dict(sock, response)


    def get_file_list(self, sock, request):
        user_name = request['user_name']
        matched_files = []
        for file_name in self.key2info.keys():
            if file_name.split('_')[0] == user_name:
                matched_files.append(file_name)
        network.send_dict(sock, {'response': matched_files})

    def join_cluster(self, sock, request):
        chunck_address = request['address']
        chunck_address = tuple(chunck_address)
        if chunck_address not in self.chunck_servers:
            self.chunck_servers.append(chunck_address)
        self.chunck_last_heart_beat_time[chunck_address] = time.time()
        print(chunck_address, 'joined cluster successfully')
        print('current chuncks:', self.chunck_servers)
        network.send_dict(sock, {'response':'success'})

    def get(self, request):
        key = request.get('key')
        info = self.key2info.get(key)
        if info == None:
            response = {'response':'no_such_file'}
        else:
            addresses = info.get('addresses')
            if (addresses == None) or (len(addresses) == 0):
                response = {'response':'no_such_file'}
            else:
                response = {'command':'get_response', 'response': info}
        return response
        

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', type=str, default='0.0.0.0', help='Master will listen this ip')
    parser.add_argument('--port', type=int, default=14477, help='Master will listen this tcp port')
    opt = parser.parse_args()
    master = Master((opt.ip, opt.port))
    master.run_server()

if __name__ == '__main__':
    main()

