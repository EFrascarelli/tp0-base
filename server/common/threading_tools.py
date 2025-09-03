# common/threading_tools.py
import threading
from contextlib import contextmanager
from typing import Callable, Optional, Any


# Reentrant lock global para sincronizar estructuras compartidas del servidor
_global_lock = threading.RLock()


def run_in_thread(target: Callable[..., Any], *args: Any, daemon: bool = True, **kwargs: Any) -> threading.Thread:
    """
    Lanza `target(*args, **kwargs)` en un thread separado.
    Retorna el objeto Thread por si querés .join() más tarde.
    Por defecto el thread es daemon (no bloquea el cierre del proceso).
    """
    t = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=daemon)
    t.start()
    return t


@contextmanager
def synchronized(lock: Optional[threading.RLock] = None):
    """
    Ejecuta el bloque dentro de una región crítica protegida por un lock.
    Si no pasás lock, usa un lock global compartido por todo el proceso.

    Uso:
        from common.threading_tools import synchronized
        with synchronized():
            # sección crítica

        # O con un lock propio:
        mylock = new_lock()
        with synchronized(mylock):
            # sección crítica
    """
    lk = lock if lock is not None else _global_lock
    lk.acquire()
    try:
        yield
    finally:
        lk.release()


def new_lock() -> threading.RLock:
    """
    Crea y retorna un RLock nuevo (reentrante).
    Útil si querés locks separados por recurso/estructura.
    """
    return threading.RLock()


class AtomicCounter:
    """
    Contador atómico simple (thread-safe).
    Ideal para métricas o conteos de eventos concurrentes.
    """
    def __init__(self, initial: int = 0):
        self._value = initial
        self._lock = threading.Lock()

    def inc(self, delta: int = 1) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def dec(self, delta: int = 1) -> int:
        with self._lock:
            self._value -= delta
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value

    def reset(self, value: int = 0) -> None:
        with self._lock:
            self._value = value