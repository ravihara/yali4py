from .abc_store import AbstractStore, AwsS3StoreConfig


class AwsS3Store(AbstractStore):
    def __init__(self, config: AwsS3StoreConfig):
        super().__init__(config)
