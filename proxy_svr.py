# -*- coding: utf-8 -*-
#auth yx

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import socket
import time
import threading
import logging
from logging.handlers import RotatingFileHandler

server_hb_pkg = '___31415926___1___hb'
client_hb_pkg = '___31415926___1___chb'
socket_read_timeout = 20
client_seq_lock = threading.Lock()
g_service_port = 29999

fog_long_link_read_buff = ''

logging.basicConfig(level=logging.INFO)
RLogger = RotatingFileHandler('proxy.log', maxBytes=5*1024*1024, backupCount=5)
RLogger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime) s %(message)s')
RLogger.setFormatter(formatter)
logging.getLogger('').addHandler(RLogger)

class SafeSock():
    def __init__(self, port = 0):
        self.socket = None
        self.read_mutex = threading.Lock()
        self.write_mutex = threading.Lock()
        self.remote_addr = None
        self.port = port
        self.closed = True
        self.last_hb_req_time = 0.0
        self.read_buffer = ''

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
            if self.closed:
                return 0
            self.write_mutex.acquire()
            self.socket.send(data)
            return len(data)
        except Exception,e:
            return 0
        finally:
            self.write_mutex.release()

    def read(self, blen = 10240):
        try:
            if self.closed:
                return None
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
g_client_seq = 1

def proxy_thread_function(long_sock, client_listen_sock, svcPort):
    global g_client_seq
    global client_seq_lock
    while 1:
        client = client_listen_sock.accept()
        if long_sock.closed:
            logging.error('proxy %s accept but fog long link %s closed' % (svcPort, svcPort))
            if client:
                client.close()
            client_listen_sock.close()
            break
        if client:
            client_seq_lock.acquire()
            seq = g_client_seq
            g_client_seq += 1
            client_seq_lock.release()
            logging.info('proxy %s accept a client(%s:%s), proxy serial No is: %s' % (svcPort, client.remote_addr[0], client.remote_addr[1], seq))
            long_sock.write('___31415926___2___%s' % seq)
            g_client_socket_dic.set('%s' % seq, client)

def sock_pair_read_thread_function(frome,to,read_buf,read_buf_lock,read_buf_sema,seq,logstr):
    while 1:
        if to.closed:
            logging.error('proxy %s rthread to is closed. %s'% (seq,logstr))
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
                logging.info('proxy %s rthread frome read excption, %s. %s' % (seq, e.message, logstr))
                frome.close()
                break
    read_buf_sema.release()
    logging.info('proxy %s frome rthread end. %s' % (seq, logstr))

def sock_pair_write_thread_function(to,frome,write_buf,write_buf_lock,write_buf_sema,seq,logstr):
    while 1:
        if frome.closed:
            logging.error('proxy %s wthread frome is closed. %s' % (seq, logstr))
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
                    sended = to.write(data)
                    if sended == 0:
                        raise 'send return 0'
            except Exception as e:
                logging.error('proxy %s wthread send to exception, %s. %s'%(seq,e.message,logstr))
                to.close()
                break
    logging.info('proxy %s wthread to end. %s' % (seq,logstr))

def sock_pair_function(client, server, seq):
    client_read_buf = []
    client_read_buf_lock = threading.Lock()
    client_read_buf_semaphore = threading.Semaphore(0)
    svr_read_buf = []
    svr_read_buf_lock = threading.Lock()
    svr_read_buf_semaphore = threading.Semaphore(0)
    client.settimeout(301)
    server.settimeout(300)
    client_read_thread = threading.Thread(target=sock_pair_read_thread_function, args=(client,server,client_read_buf,client_read_buf_lock,client_read_buf_semaphore,seq,'[from:client, to:server]'))
    client_write_thread = threading.Thread(target=sock_pair_write_thread_function, args=(server,client,client_read_buf,client_read_buf_lock,client_read_buf_semaphore,seq,'[from:client, to:server]'))
    svr_read_thread = threading.Thread(target=sock_pair_read_thread_function, args=(server,client,svr_read_buf,svr_read_buf_lock,svr_read_buf_semaphore,seq,'[from:server, to:client]'))
    svr_write_thread = threading.Thread(target=sock_pair_write_thread_function, args=(client,server,svr_read_buf,svr_read_buf_lock,svr_read_buf_semaphore,seq,'[from:server, to:client]'))
    client_read_thread.start()
    client_write_thread.start()
    svr_read_thread.start()
    svr_write_thread.start()
    logging.info('proxy %s start' % seq)

def server_function(svcSock):
    try:
        svcSock.settimeout(15)
        data = svcSock.read(64)
        if data.startswith('___31415926___0___'):
            parts = data.split('___')
            logging.info('fog long link %s comes, pkg: %s' % (parts[3], data))
            svcSock.write('___31415926___0___%s:%s' % svcSock.remote_addr)
            proxy_client = SafeSock(int(parts[3]))
            proxy_client.makeListenSock()
            proxy_client.settimeout(60)
            t = threading.Thread(target=proxy_thread_function, args=(svcSock, proxy_client, parts[3]))
            t.start()
            tr = threading.Thread(target=server_hb_write_function,args=(svcSock,parts[3]))
            tr.start()
            tw = threading.Thread(target=client_hb_read_function, args=(svcSock,parts[3]))
            tw.start()
        elif data.startswith('___31415926___2___'):
            parts = data.split('___')
            logging.info('fog proxy link comes, pkg: %s' % data)
            client_sock = g_client_socket_dic.get(parts[3])
            if client_sock:
                logging.info('start to proxy, client addr (%s:%s)' % client_sock.remote_addr)
                g_client_socket_dic.remove(parts[3])
                sock_pair_function(client_sock, svcSock, parts[3])
            else:
                logging.info('can not found client socket after proxy link comes')
                svcSock.close()
        else:
            raise "illega pkg"
    except Exception,e:
        logging.error('fog link exception, %s' % e.message)
        svcSock.close()

def server_hb_write_function(svcSock, svcPort):
    try:
        while 1:
            time.sleep(50)
            if not svcSock.closed:
                logging.debug('fog long link %s send svr hb' % svcPort)
                svcSock.write(server_hb_pkg)
                svcSock.last_hb_req_time = time.time()
            else:
                logging.info('fog long link %s write break' % svcPort)
                break
    except Exception,e:
        svcSock.close()



def client_hb_read_function(svcSock, svcPort):
    start_read_time = time.time()
    while 1:
        try:
            if not svcSock.closed:
                start_read_time = time.time()
                data = svcSock.read(1024)
                if len(svcSock.read_buffer) > 0:
                    data = svcSock.read_buffer + data
                    svcSock.read_buffer = ''
                logging.debug('fog long link %s rcv data: %s' % (svcPort, data))
                if data:
                    while len(data):
                        svcSock.last_hb_req_time = 0.0
                        if data.startswith(client_hb_pkg):
                            logging.debug('fog long link %s client hb rcv' % svcPort)
                            data = data[len(client_hb_pkg):]
                            svcSock.write(client_hb_pkg)
                        elif data.startswith(server_hb_pkg):
                            logging.debug('fog long link %s svr hb rcv' % svcPort)
                            data = data[len(server_hb_pkg):]
                        elif len(data) > len(server_hb_pkg):
                            logging.error('fog long link %s rcv unknown pkg' % svcPort)
                            svcSock.close()
                            break
                        else:
                            svcSock.read_buffer = data
                            logging.info('fog long link %s rcv half data' % svcPort)
                else:
                    svcSock.close()
            else:
                logging.info('fog long link %s closed' % svcPort)
                break;
        except Exception as e:
            if svcSock.last_hb_req_time > 1 and start_read_time > svcSock.last_hb_req_time:
                logging.info('fog long link %s hb timeout, %s' % (svcPort, e.message))
                svcSock.close()

if __name__ == '__main__':
    logging.info('proxy start, port:%s' % g_service_port)
    listen_sock = SafeSock(g_service_port)
    listen_sock.makeListenSock()
    while 1:
        try:
            slong = listen_sock.accept()
            if slong:
                logging.info('accept a link, remote address is %s:%s' % slong.remote_addr)
                t = threading.Thread(target=server_function, args=(slong,))
                t.start()
        except Exception, e:
            logging.error('accept exception, %s' % e.message)





