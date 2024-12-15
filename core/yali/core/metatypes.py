from threading import Lock
from typing import Any


class SingletonMeta(type):
    """
    SingletonMeta meta-type used to create singleton classes
    """

    __instances = {}
    __lock: Lock = Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Method use to create callable class-objects, by the enclosing meta-class (type)
        """
        if cls not in cls.__instances:
            with cls.__lock:
                if cls not in cls.__instances:
                    instance = super().__call__(*args, **kwargs)

                    @classmethod
                    def get_instance(cls):
                        return instance

                    setattr(cls, "get_instance", get_instance)
                    cls.__instances[cls] = instance

        return cls.__instances[cls]


class RiSingletonMeta(type):
    """
    RiSingletonMeta meta-type used to create singleton classes with
    the ability to reinitialize the instance
    """

    __instances = {}
    __inst_lock: Lock = Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Method use to create callable class-objects, by the enclosing meta-class (type)
        """
        is_fresh = False

        if cls not in cls.__instances:
            with cls.__inst_lock:
                if cls not in cls.__instances:
                    is_fresh = True
                    instance = super().__call__(*args, **kwargs)

                    @classmethod
                    def get_instance(cls):
                        return instance

                    setattr(cls, "get_instance", get_instance)
                    cls.__instances[cls] = instance

        instance = cls.__instances[cls]

        if not is_fresh:
            instance.__init__(*args, **kwargs)

        return instance
