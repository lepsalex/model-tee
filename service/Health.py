import threading
from flask import Flask
from service.CircuitBreaker import CircuitBreaker


class Health:
    app = Flask(__name__)
    cb = CircuitBreaker(3, 7)

    @app.route('/health')
    def health():
        Health.cb.update()
        return {
            "is_healthy": bool(Health.cb.is_blown == False),
            "recent_error_count": int(Health.cb.error_count)
        }

    @classmethod
    def run(cls):
        server = threading.Thread(target=cls.app.run)
        server.setDaemon(True)
        server.start()
