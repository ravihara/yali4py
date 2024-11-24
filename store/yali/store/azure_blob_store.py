from .abc_store import AbstractStore, AzureBlobStoreConfig
from concurrent.futures import as_completed


class AzureBlobStore(AbstractStore):
    def __init__(self, config: AzureBlobStoreConfig):
        super().__init__(config)
