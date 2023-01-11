import os

# работа с файловой системой: создание, удаление, изменение директорий и файлов;
from typing import Generator


def create_dir(name: str) -> Generator[str, None, None]:
    if not os.path.exists(name):
        os.mkdir(name)
        yield f"folder {name} is create"
    yield f"folder {name} already exists"


def rename_dir(old_name: str, new_name: str) -> Generator[str, None, None]:
    if os.path.exists(old_name):
        os.renames(old_name, new_name)
        yield f"folder {old_name} renamed to {new_name}"
    yield f"folder {old_name} not exists"


def delete_dir(name: str) -> Generator[str, None, None]:
    if os.path.exists(name):
        os.rmdir(name)
        yield f"folder {name} is delete"
    yield f"folder {name} not exists"
