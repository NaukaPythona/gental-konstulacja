from modules.model import DBModel
from modules.query import Query
from modules.database import *
from modules.path import Path


DBModel.dbs_path = Path("./data/")


@DBModel.model("name")
class User:
    name: str
    age: int = 18
    attrs: list = NOT_REQUIRED
        

users_db = Database(User)    

    
# -- Dodawanie.
u1 = User("tomek", 12, [1])
u2 = User("stas", 20)

u1_key = users_db.insert(u1)
u2_key = users_db.insert(u2)

# -- Edytowanie.
print(f"Wiek przed: {u1.age}")
users_db.update(u1_key, {"age": 18})
users_db.increment(u1_key, "age")
u1 = users_db.get(u1_key)
print(f"Wiek po: {u1.age}")

print(f"\nAtr. przed: {u2.attrs}")
users_db.update(u2_key, {"attrs": 2}, iter_append=True)
users_db.update(u2_key, {"attrs": 3}, iter_append=True)
users_db.update(u2_key, {"attrs": 2}, iter_pop=True)
u2 = users_db.get(u2_key)
print(f"Atr. po: {u2.attrs}")

# -- Usuwanie.
users_db.delete(u2_key)

