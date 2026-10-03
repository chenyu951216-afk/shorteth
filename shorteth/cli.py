"""Start the control website or run the built-in automated tests."""
import argparse
import ipaddress
import os
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

def lan_ip():
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as s:
        s.connect(('8.8.8.8',80))
        return s.getsockname()[0]

def local_certificate(data_dir,ip):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes,serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    root=Path(data_dir);root.mkdir(parents=True,exist_ok=True)
    key_path,cert_path=root/'local-https.key',root/'local-https.crt'
    if not key_path.exists() or not cert_path.exists():
        key=rsa.generate_private_key(public_exponent=65537,key_size=3072)
        now=datetime.now(timezone.utc)
        name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'SHORTETH 區域網路控制台')])
        cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name)
              .public_key(key.public_key()).serial_number(x509.random_serial_number())
              .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=365))
              .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'),
                   x509.IPAddress(ipaddress.ip_address('127.0.0.1')),
                   x509.IPAddress(ipaddress.ip_address(ip))]),critical=False)
              .sign(key,hashes.SHA256()))
        key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()))
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return cert_path,key_path

def main():
    parser=argparse.ArgumentParser(description='SHORTETH 策略控制台')
    parser.add_argument('command',choices=['web','test'])
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8766)
    parser.add_argument('--lan',action='store_true',help='以區域網路 HTTPS 提供手機瀏覽')
    args=parser.parse_args()
    if args.command=='test':
        return subprocess.call([sys.executable,'-m','pytest','-q','tests'])
    import uvicorn
    if args.lan:
        ip=lan_ip();cert,key=local_certificate(
            os.environ.get('SHORTETH_DATA_DIR',str(Path.home()/'.shorteth')),ip)
        print(f'手機與電腦同網路時開啟：https://{ip}:{args.port}/')
        print('首次設定管理密碼請先在此電腦開啟：https://127.0.0.1:'+str(args.port)+'/')
        uvicorn.run('shorteth.web:app',host='0.0.0.0',port=args.port,
                    ssl_certfile=str(cert),ssl_keyfile=str(key),log_level='info')
    else:
        if args.host not in {'127.0.0.1','::1','localhost'}:
            parser.error('遠端連線請使用 --lan 的 HTTPS 模式')
        uvicorn.run('shorteth.web:app',host=args.host,port=args.port,log_level='info')
    return 0

if __name__=='__main__':raise SystemExit(main())
