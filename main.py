"""Cloud entry point: Zeabur terminates HTTPS; one worker owns the trading loop."""
import os
import secrets
from pathlib import Path

if __name__ == '__main__':
    os.environ.setdefault('SHORTETH_CLOUD', '1')
    os.environ.setdefault('SHORTETH_DATA_DIR', '/data')
    root = Path(os.environ['SHORTETH_DATA_DIR'])
    root.mkdir(parents=True, exist_ok=True)
    if not (root / 'auth.json').exists():
        token = root / 'setup-token'
        if not token.exists():
            fd = os.open(token, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as f:
                f.write(secrets.token_urlsafe(32))
        print('SHORTETH 首次設定碼（設定完成後失效）：' + token.read_text(), flush=True)
    import uvicorn
    uvicorn.run('shorteth.web:app', host='0.0.0.0', port=int(os.environ.get('PORT', '8080')),
                workers=1, proxy_headers=False)
