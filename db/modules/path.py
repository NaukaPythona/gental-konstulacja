import shutil
import json
import stat
import os


class Path:
    """
    Abstract path representation.
    __str__, __repr__: return path
    __add__: Add string to path without /.
        >>> Path("C:/foo") + "bar" -> Path("C:/foobar")
    __truediv__: Add string to path separated by /.
        >>> Path("C:/foo") / "bar" -> Path("C:/foo/bar")
    __floordiv__: Add string to path separated by / and add next / at the end.
        >>> Path("C:/foo") // "bar" -> Path("C:/foo/bar/")
    """

    def __init__(self, src: str) -> None:
        src = src.replace("\\", "/")
        src = src.replace("//", "/")
        self.path = src

        if self.exists() and self.is_dir() and not self.path.endswith("/"):
            self.path += "/"

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        if self.path.endswith("/"):
            return self.path.removesuffix("/")
        return self.path

    def __add__(self, sub_path: object) -> "Path":
        if not isinstance(sub_path, (str, Path)):
            raise TypeError("Path.__add__ requires str or Path object.")

        return Path(self.path + str(sub_path))

    def __truediv__(self, sub_path: object) -> "Path":
        if not isinstance(sub_path, (str, Path)):
            raise TypeError("Path.__truediv__ requires str or Path object.")

        return Path(self.path + "/" + str(sub_path))

    def __floordiv__(self, sub_path: object) -> "Path":
        if not isinstance(sub_path, (str, Path)):
            raise TypeError("Path.__floordiv__ requires str or Path object.")

        return Path(self.path + "/" + str(sub_path) + "/")

    def exists(self) -> bool:
        """ Check if this Path exists. """
        return os.path.exists(self.path)

    def is_dir(self) -> bool:
        """ Check if path is a directory. """
        return stat.S_ISDIR(os.stat(str(self.path)).st_mode)

    def touch(self):
        """ Create directory using os.mkdir or file with open. """
        if self.exists():
            return self

        if self.path.endswith("/"):
            os.mkdir(self.path)
        else:
            open(self.path, "a+").close()

        return self

    def parent(self) -> "Path":
        """ Return this path's parent of self if None. """
        parts = self.path.split("/")
        if len(parts) < 3:
            return self

        return Path("/".join(parts[:-2]) + "/")

    def all_parents(self) -> set["Path"]:
        """ Get all parents of this path. """
        parents = []
        new_path = self

        for _ in range(len(self.path.split("/"))):
            new_path = new_path.parent()
            parents.append(new_path)

        return set(parents)

    def list_dir(self, as_str: bool = False) -> list["Path"]:
        """ Turn os.listdir items into Path objects. """
        if not self.is_dir():
            return []
        if as_str:
            return os.listdir(self.path)
        return [self / Path(p) for p in os.listdir(self.path)]

    def get_name(self) -> str:
        """ Return name of final item of this path. """
        if self.path.endswith("/"):
            return self.path.split("/")[-2]
        return self.path.split("/")[-1]

    def remove(self) -> None:
        """ Remove this object. """
        if not self.exists():
            return

        if self.is_dir():
            shutil.rmtree(self.path, ignore_errors=True)
        else:
            os.remove(self.path)

    def get_size(self) -> int:
        """ Returns object size in bytes. (0 if dir) """
        if self.is_dir():
            return 0

        return os.path.getsize(str(self.path))

    def write(self, content: str, mode: str = "a") -> None:
        """ Write content to file. """
        with open(self.path, mode, encoding="utf8") as file:
            file.write(content)

    def read(self) -> str:
        """ Returns file's content. ("" if dir) """
        if self.is_dir():
            return ""

        with open(self.path, "r", encoding="utf8") as file:
            return file.read()

    def get_json_content(self) -> dict:
        """ Return content of a JSON file. """
        return json.loads(self.read())

    def save_json_content(self, content: dict | list) -> None:
        """ Save provided content with JSON encoding. """
        with open(self.path, "w", encoding="utf8") as file:
            json.dump(content, file, indent=2, separators=(',', ': '), ensure_ascii=False)
