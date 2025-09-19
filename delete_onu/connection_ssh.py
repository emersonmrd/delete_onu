import paramiko
from dotenv import load_dotenv
import os

load_dotenv()

PORT = 22
LOGIN = os.getenv("LOGIN")
PASSWORD = os.getenv("PASSWORD")

def ssh(host):
    conn = paramiko.SSHClient()
    conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    conn.connect(
        hostname=host,
        port=PORT,
        username=LOGIN,
        password=PASSWORD,
        timeout=60
    )
    
    shell = conn.invoke_shell()
    return conn, shell