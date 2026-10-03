"""Password-encrypted credentials and durable, deduplicated exchange evidence."""
import base64
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from decimal import Decimal
from cryptography.fernet import Fernet


def vault_key(password, salt):
    return base64.urlsafe_b64encode(hashlib.pbkdf2_hmac('sha256', password.encode(),
        bytes.fromhex(salt), 600_000, dklen=32))


def atomic_write(path, data):
    path = Path(path)
    temp = path.with_suffix(path.suffix + '.tmp')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'wb') as f:
        f.write(data); f.flush(); os.fsync(f.fileno())
    os.replace(temp, path)


def save_credentials(root, key, profile):
    atomic_write(Path(root) / 'credentials.enc', Fernet(key).encrypt(json.dumps(profile).encode()))


def load_credentials(root, key):
    path = Path(root) / 'credentials.enc'
    return json.loads(Fernet(key).decrypt(path.read_bytes())) if path.exists() else None


class Journal:
    def __init__(self, root):
        self.path = Path(root) / 'records.sqlite'
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, at REAL, payload TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS evidence(scope TEXT, kind TEXT, id TEXT, at REAL, payload TEXT, PRIMARY KEY(scope,kind,id))')

    def db(self):
        return sqlite3.connect(self.path, timeout=15)

    def event(self, event):
        with self.db() as db:
            db.execute('INSERT INTO events(at,payload) VALUES(?,?)', (time.time(), json.dumps(event, ensure_ascii=False)))

    def events(self):
        with self.db() as db:
            return [json.loads(x[0]) for x in db.execute('SELECT payload FROM events ORDER BY id DESC LIMIT 100')]

    def put(self, scope, kind, identity, value):
        with self.db() as db:
            db.execute('INSERT OR REPLACE INTO evidence VALUES(?,?,?,?,?)',
                       (scope,kind,str(identity),time.time(),json.dumps(value,ensure_ascii=False)))

    def rows(self, scope, kind):
        with self.db() as db:
            suffix=' LIMIT 20' if kind=='snapshot' else ''
            return [json.loads(x[0]) for x in db.execute(
                'SELECT payload FROM evidence WHERE scope=? AND kind=? ORDER BY at DESC'+suffix,(scope,kind))]

    def summary(self, scope):
        rows=self.rows(scope,'position')
        known=[x for x in rows if x.get('netProfit') not in (None,'')]
        return {'已保存平倉紀錄':len(rows),'已知淨利筆數':len(known),
                '已知平倉淨利合計U':str(sum((Decimal(str(x['netProfit'])) for x in known),Decimal(0))),
                '缺少淨利的紀錄數':len(rows)-len(known),
                '說明':'交易所 ETH 帳戶歷史，可能含手動交易與最小單測試；非純策略績效。入金未計入。只涵蓋已同步保存範圍，缺值不算零。',
                '平倉紀錄':rows,'交易所委託':self.rows(scope,'order'),
                '最近帳戶快照':self.rows(scope,'snapshot')[:20]}
