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

cloud_ip = '104.224.166.34'
cloud_port = 29999
services = {'22':22,}
base_port = 40000

server_hb_pkg = '___31415926___1___hb'
client_hb_pkg = '___31415926___1___chb'


logging.basicConfig(level=logging.INFO)
RLogger = RotatingFileHandler('fog.log', maxBytes=5*1024*1024, backupCount=5)
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

    def makeConnectedSock(self, ip, port):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.settimeout(15)
            self.socket.connect((ip, port))
            self.closed = False
        except Exception as e:
            self.socket.close()

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


def sock_pair_read_thread_function(frome,to,read_buf,read_buf_lock,read_buf_sema,seq, logstr):
    while 1:
        if to.closed:
            logging.error('rthread proxy %s to closed when read frome. %s' % (seq,logstr))
            frome.close()
            break
        else:
            try:
                data = frome.read()
                if data and len(data):
                    with read_buf_lock:
                        read_buf.append(data)
                    read_buf_sema.release()
                else:
                    logging.error('rthread proxy %s frome close. %s' % (seq,logstr))
                    frome.close()
                    break
            except Exception as e:
                logging.error('rthread proxy %s frome excption:%s. %s' % (seq, e.message,logstr))
                frome.close()
                break
    read_buf_sema.release()
    logging.info('rthread proxy %s frome end. %s' % (seq,logstr))

def sock_pair_write_thread_function(to,frome,write_buf,write_buf_lock,write_buf_sema,seq,logstr):
    while 1:
        if frome.closed:
            logging.error('wthread proxy %s frome closed when write. %s' % (seq,logstr))
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
                logging.error('wthread proxy %s to write excption:%s. %s' % (seq, e.message, logstr))
                to.close()
                break
    logging.info('wthread proxy %s to end. %s' % (seq,logstr))

def start_sock_pair_proxy(local,remote,seq):
    client_read_buf = []
    client_read_buf_lock = threading.Lock()
    client_read_buf_semaphore = threading.Semaphore(0)
    svr_read_buf = []
    svr_read_buf_lock = threading.Lock()
    svr_read_buf_semaphore = threading.Semaphore(0)
    local.settimeout(301)
    remote.settimeout(300)
    client_read_thread = threading.Thread(target=sock_pair_read_thread_function, args=(
    remote, local, client_read_buf, client_read_buf_lock, client_read_buf_semaphore,seq,'[from:cloud,to:local]'))
    client_write_thread = threading.Thread(target=sock_pair_write_thread_function, args=(
    local, remote, client_read_buf, client_read_buf_lock, client_read_buf_semaphore,seq,'[from:cloud,to:local]'))
    svr_read_thread = threading.Thread(target=sock_pair_read_thread_function,
                                       args=(local, remote, svr_read_buf, svr_read_buf_lock, svr_read_buf_semaphore,seq,'[from:local,to:cloud]'))
    svr_write_thread = threading.Thread(target=sock_pair_write_thread_function,
                                        args=(remote, local, svr_read_buf, svr_read_buf_lock, svr_read_buf_semaphore,seq,'[from:local,to:cloud]'))
    client_read_thread.start()
    client_write_thread.start()
    svr_read_thread.start()
    svr_write_thread.start()

def start_proxy_link_thread_function(seq, local_port):
    local_link = SafeSock()
    local_link.makeConnectedSock('127.0.0.1', local_port)
    if not local_link.closed:
        remote_link = SafeSock()
        remote_link.makeConnectedSock(cloud_ip, cloud_port)
        if not remote_link.closed:
            logging.info('start proxy, seq:%s, local:%s'% (seq, local_port))
            remote_link.write('___31415926___2___%s' % seq)
            start_sock_pair_proxy(local_link, remote_link,seq)
        else:
            logging.error('start proxy fail, connect remote fail, seq:%s, local:%s' % (seq, local_port))
            local_link.close()
    else:
        logging.error('start proxy fail, connect local fail, seq:%s, local:%s' % (seq, local_port))

def proxy_hb_write_thread_function(lsock,local_port):
    try:
        while 1:
            time.sleep(70)
            if not lsock.closed:
                lsock.write(client_hb_pkg)
                lsock.last_hb_req_time = time.time()
            else:
                logging.error('fog long link %s, hb want write ,but closed' % local_port)
                break
    except Exception as e:
        logging.error('fog long link %s, write hb exception:%s' % (local_port, e.message))
        lsock.close()

def proxy_hb_read_thread_function(lsock, local_port):
    start_read_time = time.time()
    while 1:
        try:
            if not lsock.closed:
                start_read_time = time.time()
                data = lsock.read(1024)
                if len(lsock.read_buffer):
                    data = lsock.read_buffer + data
                    lsock.read_buffer = ''
                if data:
                    while len(data):
                        lsock.last_hb_req_time = 0.0
                        if data.startswith(server_hb_pkg):
                            data = data[len(server_hb_pkg):]
                            lsock.write(server_hb_pkg)
                        elif data.startswith(client_hb_pkg):
                            data = data[len(client_hb_pkg):]
                        elif data.startswith('___31415926___2___'):
                            parts = data.split('___')
                            data = data[len('___31415926___2___') + len(parts[3]):]
                            sequence = int(parts[3])
                            start_proxy_thread = threading.Thread(target=start_proxy_link_thread_function, args=(sequence,local_port))
                            start_proxy_thread.start()
                        elif len(data) > client_hb_pkg:
                            logging.error('fog long link %s rcv pkg illegal:%s' % (local_port, data))
                            lsock.close()
                        elif not data.startswith('___31415926___'):
                            logging.error('fog long link %s rcv pkg illegal:%s' % (local_port, data))
                            lsock.close()
                        else:
                            lsock.read_buffer = data
                else:
                    logging.error('fog long link %s read 0, close' % local_port)
                    lsock.close()
            else:
                logging.error('fog long link %s read loop closed break' % local_port)
                break
        except Exception as e:
            if lsock.last_hb_req_time > 1 and start_read_time > lsock.last_hb_req_time:
                logging.error('fog long link %s hb timeout, e:%s' % (local_port, e.message))
                lsock.close()
    time.sleep(30)
    t = threading.Thread(target=service_thread_function, args=(local_port,))
    t.start()

def service_thread_function(port):
    lsock = SafeSock()
    lsock.makeConnectedSock(cloud_ip,cloud_port)
    if not lsock.closed:
        lsock.write('___31415926___0___%s' % (port + base_port))
        try:
            data = lsock.read(64)
            logging.info('fog long link start, port:%s, rcv data:%s' % (port, data))
            if data and data.startswith('___31415926___0___'):
                w_thread = threading.Thread(target=proxy_hb_write_thread_function, args=(lsock,port))
                r_thread = threading.Thread(target=proxy_hb_read_thread_function,args=(lsock,port))
                w_thread.start()
                r_thread.start()
            else:
                raise 'remote error'
        except Exception as e:
            logging.error('fog long link, port:%s start exception:%s' % (port, e.message))
            lsock.close()
            time.sleep(30)
            t = threading.Thread(target=service_thread_function,args=(port,))
            t.start()
    else:
        logging.error('fog long link,port:%s connect cloud fail' % port)
        time.sleep(30)
        t = threading.Thread(target=service_thread_function, args=(port,))
        t.start()


if __name__ == '__main__':
    for key in services:
        logging.info('fog start long connection on %s:%s' % (key,services[key]))
        t = threading.Thread(target=service_thread_function,args=(services[key],))
        t.start()

