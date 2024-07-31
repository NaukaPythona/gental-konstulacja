"""
MODULE
    database.py

DESCRIPTION
    Custom key-value, json based database system.

    Database:
      Database must be initialized from model which type is called T_Model.
      Each database contains ONLY ONE column, name and it's file path.
      After initialization, DB's object is saved into register.
      Register prevents reinitialization and allows quick DB access.
      You can get initialized Database object by calling Database.get_database(name).

      Interface methods:
          - insert(data: T_Model) -> str
            Inserts new row to database, returns provided key.
          - update(key: str, changes: dict[str, Any], iter_append: bool = False, iter_pop: bool = True)
            Updates specified in changes parameter values. Append/pop from iterable if flag is set.
          - get(key: str) -> T_Model
            Returns T_Model object with data from database.
          - increment(key: str, column_name: str) -> bool
            Increment value of number-like field.
          - decrement(key: str, column_name: str) -> bool
            Decrement value of number-like field.
          - get_all_models() -> List[T_Model]
            Returns list of all models saved in database.
          - get_all_keys() -> List[str]
            Returns list of all keys saved in database.
"""
from modules.model import DBModel
from modules.query import Query

from dataclasses import dataclass, asdict
from typing import Any, Iterable, List
import hashlib
import uuid
import json


NOT_REQUIRED = "_NOTREQ"
KEY_AS_UUID4 = "_UUID4KEY"
EXACT_KEY = lambda key: f"_EXACT:{key}"
UNDEFINED_DEFAULT_VALUE = NOT_REQUIRED
SET_AFTER_INIT = "_SET_AFTER_INIT"


class KeyNotFound(Exception):
    """
    Exception raised when provided key
    has not been found in database.
    """


class RequiredValueNotProvided(Exception):
    """
    Exception raised when value for required
    column wasn't provided
    """


def parse_key_provider(key_provider: str, model) -> str:
    """ Create db_key from model and it's key_provider. """    
    if key_provider == KEY_AS_UUID4:
        return uuid.uuid4().hex
    
    if key_provider.startswith("_EXACT:"):
        return key_provider.removeprefix("_EXACT:")

    no_hash = key_provider.startswith("!")
    key_provider = key_provider.removeprefix("!")

    if "+" in key_provider:
        attributes = key_provider.split("+")
        key_seed = ""
        for attr in attributes:
            key_seed += getattr(model, attr)
    else:
        key_seed = str(getattr(model, key_provider))

    if no_hash:
        return key_seed

    return hashlib.sha1(key_seed.encode()).hexdigest()



@dataclass
class Column:
    """
    Representation of single model's column.

    date_join: int = None
    ^^^^^^^^^  ^^^   ^^^^
    name       type_ default
    """
    name: str
    type_: type
    default: Any = UNDEFINED_DEFAULT_VALUE

    def __repr__(self) -> str:
        return f"<Column: name={self.name} type:{self.type_} default:{self.default}>"

    def prepare_value(self, value: Any) -> Any:
        """ Cast value to required type if is not. """
        if value == NOT_REQUIRED or value is None:
            value = self.type_() if self.default == UNDEFINED_DEFAULT_VALUE else self.default

        elif not isinstance(value, self.type_):
            value = self.type_(value)

        return value

    def validate(self, value: Any) -> bool:
        """ Check if value's type is same as required. """
        return isinstance(value, self.type_)


class Database:
    register: dict[str, "Database"] = {}

    def __init__(self, model: DBModel):
        self.__model: DBModel = model.__dbmodel__
        self.name = self.__model.name
        self.filepath = self.__model.file_path
        self.key_provider = self.__model.key_provider
        self.allow_invalid_values = self.__model.allow_invalid_values
        self.dump_on_error = self.__model.dump_on_error
        self.columns: dict[str, Column] = {}

        if self.name in Database.register:
            self = Database.register.get(self.name)
            return

        self.__ensure_db_file()
        self.__build_from_model()
        Database.register[self.name] = self

    def __repr__(self) -> str:
        return f"<DB: name={self.name} keyProvider={self.key_provider} columns={set(self.columns.keys())} file={self.filepath}>"

    def __build_from_model(self) -> None:
        """
        Read and parse all data from DB's model provided at initialization.
        Reads model's properties and turns each key into Column object.
        """
        object_fields = self.__model.fields
        for field_name, field_type in object_fields.items():
            if hasattr(self.__model.model_cls, field_name):
                default_value = getattr(self.__model.model_cls, field_name)
            else:
                default_value = UNDEFINED_DEFAULT_VALUE

            column = Column(field_name, field_type, default_value)
            self.columns[field_name] = column
            
    def __ensure_db_file(self) -> None:
        """ Check and create blank DB file if not exists. """
        if not self.filepath.exists():
            self.filepath.touch()
            self.filepath.save_json_content({})
            return

        try:
            self.__get_db_content()

        except json.JSONDecodeError:
            if not self.dump_on_error:
                raise

            corrupted_content = self.filepath.read()
            dumpfile_content = "\n\n--- DUMP ---\n" + corrupted_content
            (self.filepath + ".dump").touch().write(dumpfile_content)
            self.filepath.save_json_content({})

    def __get_db_content(self) -> dict:
        """ Get and return database's file content as dict. """
        return self.filepath.get_json_content()

    def __save_model(self, model: DBModel, db_key: str | None = None) -> str:
        """
        Write entry to database. If key is not provided,
        new entry will be created with provided key.
        Returns database key.
        """
        if db_key is None:
            db_key = parse_key_provider(self.key_provider, model)

        content = {}
        for column_name, value in asdict(model).items():
            column = self.columns.get(column_name)
            value = column.prepare_value(value)

            if not column.validate(value):
                # logs.database_logger.log(self.name, f"Value: <{value}> did not pass column's validation.")

                if column.default != UNDEFINED_DEFAULT_VALUE:
                    value = column.default
                    # logs.database_logger.log(self.name, f"Replaced <{value}> with default value.")

                elif self.allow_invalid_values:
                    value = column.type_()
                    # logs.database_logger.log(self.name, f"Column: {repr(column)} have no default value. Using type's default as it is allowed.")

                else:
                    # logs.database_logger.log(self.name, f"Column: {repr(column)} have no default value. Invalid values are not allowed. Model will not be saved.")
                    return

            content[column_name] = value

        db_content = self.__get_db_content()
        db_content[db_key] = content
        self.filepath.save_json_content(db_content)
        return db_key
    
    def _migrate(self) -> int:
        """
        Should be called if new column has been added to the model. 
        Adds that column into all existing entries. Column must have default value.
        Returns updates count.
        """
        raw_content = self.__get_db_content()
        new_content = {}
        changes = 0
        
        for db_key, row_db_content in raw_content.items(): 
            for column_name, column_obj in self.columns.items():
                if column_name not in row_db_content:
                    row_db_content[column_name] = column_obj.prepare_value(None)
                    changes += 1
                
            new_content[db_key] = row_db_content
            
        if changes:
            # logs.database_logger.log(f"Migration: {self.name}", f"Saving updated content with: {changes} updated rows.")
            self.filepath.save_json_content(new_content)
            
        return changes

    def insert(self, data: DBModel) -> str:
        """ Insert new entry to database. Returns key. """
        return self.__save_model(data)

    def update(self, key: str, changes: dict[str, Any] | Any, iter_append: bool = False, iter_pop: bool = False) -> None:
        """
        Update specified keys in entry.
        changes parameter does not have to contain all keys with values
          but only changed ones.

        If iter_append is set to True additional data will be appended
          to the original instead of completely replacing list with new data.

        If iter_pop is set to True value will be popped from current list
          instead of being completely replacing list new data.
        """
        if iter_append and iter_pop:
            # logs.database_logger.log(self.name, "method called with both iter_append and iter_pop flags!")
            return

        model_object = self.get(key)
        for key_name, value in changes.items():
            if not hasattr(model_object, key_name):
                # logs.database_logger.log(self.name, f"Cannot change value of {key_name} (key not found)")
                continue

            if iter_append:
                current_data = getattr(model_object, key_name)
                if isinstance(current_data, list):
                    value = current_data + [value]
                if isinstance(current_data, dict):
                    value = current_data.update(value)

            if iter_pop:
                current_data = getattr(model_object, key_name)
                if isinstance(current_data, list):
                    if value in current_data:
                        current_data.remove(value)
                        value = current_data
                    else:
                        # logs.database_logger.log(self.name, f"Cannot iter_pop {value} from {key_name} (not found)")
                        return
                if isinstance(current_data, dict):
                    current_data.pop(value)
                    value = current_data
                    

            setattr(model_object, key_name, value)

        self.__save_model(model_object, key)

    def delete(self, key: str) -> None:
        """ Delete key-value pair from database. Raises KeyNotFound. """
        db_content = self.__get_db_content()
        if key not in db_content:
            raise KeyNotFound(f"db: {self.name} key: {key}")

        db_content.pop(key)
        self.filepath.save_json_content(db_content)

    def get(self, key: str) -> DBModel:
        """
        Get object from database by it's key.
        Raises KeyNotFound error if key is invalid.
        """
        db_content = self.__get_db_content()
        object_content = db_content.get(key)
        if object_content is None:
            raise KeyNotFound(f"db: {self.name} key: {key}")

        model_object = self.__model(**object_content)
        model_object._key = key
        return model_object

    def query(self, q: Query) -> List[DBModel]:
        positive_models = []
        
        for model in self.get_all_models():
            dictmodel = asdict(model)
            # if q.column_name not in dictmodel:
            #     pass
            
            # for to_set in Query._to_set:
            #     to_set.

            
            print(q())
            # if q().resolve(dictmodel):
                # positive_models.append(model)
        
        return positive_models
        
    def increment(self, key: str, column_name: str) -> bool:
        """ 
        Increment value of field in database if it is Integer or Float. 
        Returns status. Raises KeyNotFound on invalid column_name or key.
        """
        column = self.columns.get(column_name)
        if not column:
            raise KeyNotFound(f"db: {self.name} column: {column_name}")
          
        model = self.get(key)
        if not model:
            raise KeyNotFound(f"db: {self.name} key: {key}")
        
        value = getattr(model, column_name)
        if not isinstance(value, (int, float)):
            return False
        
        value += 1
        setattr(model, column_name, value)
        self.__save_model(model, key)
        return True

    def decrement(self, key: str, column_name: str) -> bool:
        """ 
        Decrement value of field in database if it is Integer or Float. 
        Returns status. Raises KeyNotFound on invalid column_name or key.
        """
        column = self.columns.get(column_name)
        if not column:
            raise KeyNotFound(f"db: {self.name} column: {column_name}")
          
        model = self.get(key)
        if not model:
            raise KeyNotFound(f"db: {self.name} key: {key}")
        
        value = getattr(model, column_name)
        if not isinstance(value, (int, float)):
            return False
        
        value -= 1
        setattr(model, column_name, value)
        self.__save_model(model, key)
        return True

    def get_all_models(self) -> List[DBModel]:
        """ Get all models saved in database. """
        objects = []
        db_content = self.__get_db_content()
        for key, content in db_content.items():
            model = self.__model(**content)
            model._key = key
            objects.append(model)

        return objects

    def get_all_keys(self) -> List[str]:
        """ Get all keys saved in database. """
        return list(self.__get_db_content().keys())


