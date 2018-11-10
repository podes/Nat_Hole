# -*- coding: utf-8 -*-
#auth yx

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import socket
import time
import threading

server_hb_pkg = '___31415926___1___hb'
client_hb_pkg = '___31415926___1___chb'
socket_read_timeout = 20
client_seq_lock = threading.Lock()
g_service_port = 29999

class SafeSock():
    def __init__(self, port = 0):
        self.socket = None
        self.read_mutex = threading.Lock()
        self.write_mutex = threading.Lock()
        self.remote_addr = None
        self.port = port
        self.closed = True
        self.last_hb_req_time = 0.0

    def makeCommonSock(self, sock):
        self.socket = sock
        self.closed = False

    def makeListenSock(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('0.0.0.0', self.port))
        self.socket.listen(5)
        self.closed = False
    def accept(self):
        try:
            self.read_mutex.acquire()
            sock,addr = self.socket.accept()
            sfsock = SafeSock()
            sfsock.socket = sock
            sfsock.remote_addr = addr
            sfsock.closed = False
            return sfsock
        except Exception,e:
            return None
        finally:
            self.read_mutex.release()

    def write(self,data):
        try:
            self.write_mutex.acquire()
            self.socket.send(data)
            return len(data)
        except Exception,e:
            return 0
        finally:
            self.write_mutex.release()
    def read(self, blen = 10240):
        try:
            self.read_mutex.acquire()
            data = self.socket.recv(blen)
            return data
        except socket.timeout as e:
            raise e
        except Exception as e:
            return None
        finally:
            self.read_mutex.release()
    def settimeout(self, tout):
        self.socket.settimeout(tout)
    def close(self):
        if not self.closed:
            self.closed = True
            self.socket.close()

class SafeDictionary():
    def __init__(self):
        self.dic = {}
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            value = self.dic.get(key,None)
            return value

    def set(self,key,value):
        with self.lock:
            self.dic[key] = value

    def remove(self,key):
        with self.lock:
            self.dic.pop(key)

g_client_socket_dic = SafeDictionary()
g_client_seq = [1,]

def proxy_thread_function(long_sock, client_listen_sock):
    while 1:
        client = client_listen_sock.accept()
        if long_sock.closed:
            if client:
                client.close()
            client_listen_sock.close()
            break
        if client:
            seq = 0
            client_seq_lock.acquire()
            seq = g_client_seq[0]
            g_client_seq[0] += 1
            client_seq_lock.release()
            long_sock.write('___31415926___2___%s' % seq)
            g_client_socket_dic.set('%s' % seq, client)

def sock_pair_read_thread_function(frome,to,read_buf,read_buf_lock,read_buf_sema):
    while 1:
        if to.closed:
            frome.close()
            break
        else:
            try:
                data = frome.read()
                if data:
                    with read_buf_lock:
                        read_buf.append(data)
                    read_buf_sema.release()
                else:
                    frome.close()
                    break
            except Exception as e:
                frome.close()
                break

def sock_pair_write_thread_function(to,frome,write_buf,write_buf_lock,write_buf_sema):
    while 1:
        if frome.closed:
            to.close()
            break
        else:
            try:
                write_buf_sema.acquire()
                data = None
                with write_buf_lock:
                    if len(write_buf):
                        data = write_buf.pop(0)
                if data:
                    to.write(data)
            except Exception as e:
                to.close()
                break

def sock_pair_function(client, server):
    client_read_buf = []
    client_read_buf_lock = threading.Lock()
    client_read_buf_semaphore = threading.Semaphore(0)
    svr_read_buf = []
    svr_read_buf_lock = threading.Lock()
    svr_read_buf_semaphore = threading.Semaphore(0)
    client.settimeout(300)
    server.settimeout(300)
    client_read_thread = threading.Thread(target=sock_pair_read_thread_function, args=(client,server,client_read_buf,client_read_buf_lock,client_read_buf_semaphore))
    client_write_thread = threading.Thread(target=sock_pair_write_thread_function, args=(server,client,client_read_buf,client_read_buf_lock,client_read_buf_semaphore))
    svr_read_thread = threading.Thread(target=sock_pair_read_thread_function, args=(server,client,svr_read_buf,svr_read_buf_lock,svr_read_buf_semaphore))
    svr_write_thread = threading.Thread(target=sock_pair_write_thread_function, args=(client,server,svr_read_buf,svr_read_buf_lock,svr_read_buf_semaphore))
    client_read_thread.start()
    client_write_thread.start()
    svr_read_thread.start()
    svr_write_thread.start()

def server_function(svcSock):
    try:
        svcSock.settimeout(15)
        data = svcSock.read(64)
        if data.startswith('___31415926___0___'):
            parts = data.split('___')
            svcSock.write('___31415926___0___%s:%s' % svcSock.remote_addr)
            proxy_client = SafeSock(int(parts[3]))
            proxy_client.makeListenSock()
            proxy_client.settimeout(60)
            t = threading.Thread(target=proxy_thread_function, args=(svcSock, proxy_client))
            t.start()
            tr = threading.Thread(target=server_hb_write_function,args=(svcSock,))
            tr.start()
            tw = threading.Thread(target=client_hb_read_function, args=(svcSock,))
            tw.start()
        elif data.startswith('___31415926___2___'):
            parts = data.split('___')
            client_sock = g_client_socket_dic.get(parts[3])
            if client_sock:
                g_client_socket_dic.remove(parts[3])
                sock_pair_function(client_sock, svcSock)
            else:
                svcSock.close()
        else:
            raise "illega pkg"
    except Exception,e:
        svcSock.close()

def server_hb_write_function(svcSock):
    try:
        while 1:
            time.sleep(50)
            if not svcSock.closed:
                svcSock.write(server_hb_pkg)
                svcSock.last_hb_req_time = time.time()
            else:
                break
    except Exception,e:
        svcSock.close()

def client_hb_read_function(svcSock):
    start_read_time = time.time()
    while 1:
        try:
            if not svcSock.closed:
                start_read_time = time.time()
                data = svcSock.read(1024)
                if data:
                    while len(data):
                        svcSock.last_hb_req_time = 0.0
                        if data.startswith(client_hb_pkg):
                            data = data[len(client_hb_pkg):]
                            svcSock.write(client_hb_pkg)
                        elif data.startswith(server_hb_pkg):
                            data = data[len(server_hb_pkg):]
                        elif len(data) > len(server_hb_pkg):
                            svcSock.close()
                            break
                        else:
                            pass
                else:
                    svcSock.close()
            else:
                break;
        except Exception as e:
            if svcSock.last_hb_req_time > 1 and start_read_time > svcSock.last_hb_req_time:
                svcSock.close()

if __name__ == '__main__':
    listen_sock = SafeSock(g_service_port)
    listen_sock.makeListenSock()
    while 1:
        try:
            slong = listen_sock.accept()
            if slong:
                t = threading.Thread(target=server_function, args=(slong,))
                t.start()
        except Exception, e:
            print 'excption happens : %s' % e



