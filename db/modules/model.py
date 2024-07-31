from modules.path import Path

from dataclasses import dataclass
from typing import Type


class DBModel:
    dbs_path: Path = Path("./")

    @staticmethod
    def model(
        key_provider: str,
        file_path: str = None,
        allow_invalid_values: bool = None,
        dump_on_error: bool = None
    ):
        def wrapper(cls):
            name = cls.__name__
            
            nonlocal file_path
            if file_path is None:
                file_path = DBModel.dbs_path / name + ".json"

            nonlocal allow_invalid_values
            if allow_invalid_values is None:
                allow_invalid_values = True

            nonlocal dump_on_error
            if dump_on_error is None:
                dump_on_error = True

            db_model = DBModel(
                name,
                key_provider,
                file_path,
                allow_invalid_values,
                dump_on_error,
                cls
            )
            cls.__dbmodel__ = db_model
            return dataclass(cls)
        return wrapper

    def __call__(self, *args, **kwargs):
        return self.model_cls(*args, **kwargs)

    def __init__(self, name: str, key_provider: str, file_path: str, allow_invalid_values: bool, dump_on_error: bool, model_cls: Type) -> None:
        self.name = name
        self.key_provider = key_provider
        self.file_path = file_path
        self.allow_invalid_values = allow_invalid_values
        self.dump_on_error = dump_on_error

        self.model_cls = model_cls
        self.fields = self.model_cls.__annotations__

    def __repr__(self) -> str:
        model_class_name = self.__class__.__name__
        return f"<DBModel: name={self.name} key_provider={self.key_provider} file_path={self.file_path} model_class_name={model_class_name} allow_invalid_values={self.allow_invalid_values} fields={self.fields}>"
