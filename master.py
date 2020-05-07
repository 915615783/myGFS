import socket
import network
import random
import argparse

class Master():
    def __init__(self, address, m=3):
        self.address = address
        self.key2info = {}
        self.chunck_servers = []
        self.m = m
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(address)
        self.sock.listen()

    def run_server(self):
        print('Master server begins listening...', self.address)
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
                else:
                    response = {'response':'unvalid_command'}
            except Exception as e:
                print(e)
            finally:
                req_sock.close()

    def push_finish(self, sock, request):
        key = request.get('key')
        if key == None:
            return
        self.key2info[key] = request.get('info')
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
        if chunck_address not in self.chunck_servers:
            self.chunck_servers.append(chunck_address)
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

