# Nat_Hole
A simple nat hole tool, use it when you want to visit your Inner-NAT service. Use tcp proxy to connect your inner service

### How to use:
1. Run proxy_svr.py on your outter server, eg. 'nohup python proxy_svr.py &'. You can change you service port by rewrite the 'g_service_port' in the front of the source;
2. Rewite inner service info in fog_svr.py:cloud_ip、cloud_port、services = {'22':22,}、base_port = 40000;
3. Run fog_svr.py on your inner server, eg.'nohup python fog_svr.py &';
4. Use it! eg.'ssh -p(base_port+inner_port) user@(Outter IP)

分为2个部分，内网部分和公网部分。
前提条件是你已经有了一个外网服务器，就可以通过该外网服务器搭一个透明的tcp代理到你的内网服务端

### 使用：
1.在公网服务器上运行proxy_svr.py,最好deamon运行，如 'nohup python proxy_svr.py &';
2.在内网的fog_svr.py中，修改cloud_ip、cloud_port、services = {'22':22,}、base_port = 40000 这几个配置，分别是你在proxy_svr.py服务端开启的ip和端口;
3.在内网服务器上运行fog_svr.py,最好deamon运行，如 'nohup python fog_svr.py &';
4.现在你可以使用公网代理访问内网tcp服务了，比如访问ssh，就可以访问 'ssh -p(base_port+inner_port) user@(公网IP)'

### 属于闲暇时间想公网控制自己家里的设备简单做的，不保证稳定和可靠性
