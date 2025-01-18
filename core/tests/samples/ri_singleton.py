from yali.core.typebase import RiSingletonMeta


class ParentClass(metaclass=RiSingletonMeta):
    _base_value = "Parent Value"

    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value

    @classmethod
    def get_base_value(cls):
        return cls._base_value


class ChildClass(ParentClass):
    _base_name = "Child Value"

    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value

    @classmethod
    def get_base_value(cls):
        return cls._base_value


class GrandChildClass(ChildClass):
    _base_name = "Grand Child Value"

    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value

    @classmethod
    def get_base_value(cls):
        return cls._base_value
