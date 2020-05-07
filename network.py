import socket
import json
import os
BLOCKSIZE = 65535
END_SIGN = b'1p2a3nd5edda6chonggfecho4n45gtefgfally3h5ouadzxcafth1isist2he1end'  # 没有考虑转义字符，不过这么长的串，应该不会重复吧

def send_block(sock, b_data):
    '''
    成功返回None, 失败抛出异常
    b_data: (bytes)
    '''
    data = b_data + END_SIGN
    return sock.sendall(data)

def recv_block(sock, timeout=20, buffersize=65535):
    '''
    以应用层的结束符来控制结束。成功返回接收到的完整bytes数据，超过timeout时间会报错
    '''
    sock.settimeout(timeout)
    data = b''
    empty_count = 0
    while True:
        rd = sock.recv(buffersize)
        if rd == b'':
            empty_count += 1
        if empty_count > 100:
            raise Exception('The connection is closed')
        data += rd
        if data[-len(END_SIGN):] == END_SIGN:
            return data[:-len(END_SIGN)]

def send_dict(sock, d_data):
    b_data = json.dumps(d_data).encode('utf-8')
    return send_block(sock, b_data)

def recv_dict(sock, timeout=20, buffersize=65535):
    b_data = recv_block(sock, timeout, buffersize)
    return json.loads(b_data.decode('utf-8'))

def send_from_full_to_blocks(sock, file_path, block_size=BLOCKSIZE, client=None):  # 6mb一个block
    block_count = 0
    with open(file_path, 'rb') as f:
        while True:
            # print(block_count)
            block = f.read(block_size)
            if block == b'':
                send_block(sock, b'finish')
                return block_count
            send_block(sock, block)
            if recv_block(sock, timeout=300) != b'success':
                raise Exception('Sending not success when sending the %d th block'%block_count)
            if client != None:
                client.push_total_blocks_lock.acquire()
                client.push_finish_total_blocks += 1
                client.state_var.set(('上传中 %.1f' % ((client.push_finish_total_blocks / client.push_total_blocks)*100)) + '%')
                client.push_total_blocks_lock.release()
            block_count += 1

def recv_from_blocks_to_blocks(sock, save_path, timeout=20, buffersize=655350):
    '''
    save_path: 文件名的目录和前半部分
    '''
    block_count = 0
    while True:
        block = recv_block(sock, timeout, buffersize)
        if block == b'finish':
            return block_count
        with open(save_path+'_'+str(block_count)+'.block', 'wb') as f:
            f.write(block)
        block_count += 1
        send_block(sock, b'success')

def send_from_blocks_to_blocks(sock, file_path, num_blocks):
    for i in range(num_blocks):
        with open(file_path+'_'+str(i)+'.block', 'rb') as f:
            block = f.read()
            send_block(sock, block)
            # print('send block le')
            if recv_block(sock, timeout=300) == b'success':
                pass
            else:
                return
    return True

def recv_from_blocks_to_full(sock, save_path, num_blocks, timeout=20, buffersize=655350, state_var=None):
    with open(save_path, 'wb') as f:
        for i in range(num_blocks):
            if state_var != None:
                # print(i)
                state_var.set(('下载中 %.1f'%(100.0 * i/num_blocks)) + '%')
            block = recv_block(sock, timeout, buffersize)
            f.write(block)
            send_block(sock, b'success')
    return True    



