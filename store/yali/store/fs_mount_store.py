from .abc_store import AbstractStore, FsMountStoreConfig
from concurrent.futures import as_completed


class FsMountStore(AbstractStore):
    def __init__(self, config: FsMountStoreConfig):
        super().__init__(config)
