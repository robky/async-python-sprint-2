from typing import Generator

import requests


def get_users_delay(time: int = 1) -> Generator[str, None, None]:
    url = f"https://reqres.in/api/users?delay={time}"
    response = requests.get(url)
    if response.status_code == 200:
        users = response.json().get("data")
        if users:
            for user in users:
                yield (f"Get user {user.get('first_name')} "
                       f"{user.get('last_name')}")


def get_cats(number: int = 10) -> Generator[str, None, None]:
    while number > 0:
        url = "https://api.thecatapi.com/v1/images/search?limit=10"
        response = requests.get(url)
        if response.status_code == 200:
            cats = response.json()
            if cats:
                for cat in cats:
                    yield f"Get url cat: {cat.get('url')}"
                    number -= 1
                    if number == 0:
                        break
