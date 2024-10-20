import asyncio
import pickle
from logging import LogRecord
from logging.handlers import QueueHandler
from multiprocessing import Queue as LogQueue


class MprocAsyncLogHandler(QueueHandler):
    def __init__(self, queue: LogQueue):
        super().__init__(queue)

    def emit(self, record: LogRecord):
        try:
            pickle.dumps(obj=record, protocol=pickle.DEFAULT_PROTOCOL)
            self.enqueue(record=record)
        except asyncio.CancelledError:
            raise
        except (pickle.PickleError, TypeError, AttributeError):
            pass
        except Exception:
            self.handleError(record=record)
