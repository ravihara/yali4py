import asyncio
import atexit
import pickle
from logging import LogRecord, getLogger
from logging.config import dictConfig
from logging.handlers import QueueHandler
from multiprocessing import Queue as MprocQueue
from multiprocessing import current_process
from typing import Any, Dict

from ..config import yali_mproc_context
from ..consts import YALI_SENTINEL


class MprocLogQueueListener:
    def __init__(self, queue: MprocQueue[LogRecord], config: Dict[str, Any]):
        """
        Initialize a multiprocessing QueueListener

        Parameters
        ----------
        name: str
            The name of the listener
        queue: MprocQueue
            The multiprocessing queue to use
        config: Dict[str, Any]
            The actual logging configuration dictionary, typically defining console, file handlers and custom
            loggers. Note that, this should not contain 'queue_listener' handler. This class essentially
            starts a separate logging process which will handle the records from the queue.
        """
        self.queue = queue
        self.config = config
        self._log_process = None

    def dequeue(self, block: bool = True, timeout: float | None = None):
        """
        Dequeue a record and return it, optionally blocking.

        The base implementation uses get. You may want to override this method
        if you want to use timeouts or work with custom queue implementations.
        """
        return self.queue.get(block=block, timeout=timeout)

    def start(self):
        """
        Start the listener.

        This starts up a background process to monitor the queue for
        LogRecords to handle.
        """
        mproc_context = yali_mproc_context()
        self._log_process = mproc_context.Process(
            name="yali-mproc-log-listener", target=self._monitor
        )

        self._log_process.start()

    def prepare(self, record: LogRecord):
        """
        Prepare a record for handling.

        This method just returns the passed-in record. You may want to
        override this method if you need to do any custom marshalling or
        manipulation of the record before passing it to the handlers.
        """
        curr_proc_name = current_process().name

        if curr_proc_name != record.processName:
            record.processName = f"{curr_proc_name}->{record.processName}"

        return record

    def handle(self, record: LogRecord):
        """
        Handle a record.

        This just loops through the handlers offering them the record
        to handle.
        """
        record = self.prepare(record)

        if record.name == "root":
            rec_logger = getLogger()
        else:
            rec_logger = getLogger(record.name)

        for handler in rec_logger.handlers:
            if record.levelno >= handler.level:
                handler.handle(record)

    def _monitor(self):
        """
        Monitor the queue for records, and ask the handler
        to deal with them.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        dictConfig(config=self.config)

        while True:
            try:
                record = self.dequeue(True)

                if record is YALI_SENTINEL:
                    root_handler = getLogger()

                    for hndl in root_handler.handlers:
                        hndl.flush()
                        hndl.close()

                    print("Received break-event, exiting...")
                    break

                self.handle(record=record)
            except KeyboardInterrupt:
                self.enqueue_sentinel()
            except Exception:
                import sys
                import traceback

                print("Multi-process log processing failed", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    def enqueue_sentinel(self):
        """
        This is used to enqueue the sentinel record.

        The base implementation uses put_nowait. You may want to override this
        method if you want to use timeouts or work with custom queue
        implementations.
        """
        self.queue.put_nowait(YALI_SENTINEL)

    def stop(self):
        """
        Stop the listener.

        This asks the thread to terminate, and then waits for it to do so.
        Note that if you don't call this before your application exits, there
        may be some records still left on the queue, which won't be processed.
        """
        if self._log_process and self._log_process.is_alive():
            self.enqueue_sentinel()
            self._log_process.join()
            self._log_process.close()

        self._log_process = None


class MprocLogQueueHandler(QueueHandler):
    def __init__(
        self,
        log_queue: MprocQueue[LogRecord],
        log_config: Dict[str, Any],
        is_main_process: bool = False,
    ):
        super().__init__(queue=log_queue)

        if is_main_process:
            self._listener = MprocLogQueueListener(queue=log_queue, config=log_config)
            self._listener.start()
            atexit.register(self._listener.stop)

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
