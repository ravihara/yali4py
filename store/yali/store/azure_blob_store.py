from .abc_store import AbstractStore, AzureBlobStoreConfig


class AzureBlobStore(AbstractStore):
    def __init__(self, config: AzureBlobStoreConfig):
        super().__init__(config)
