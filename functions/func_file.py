from typing import Generator


def write_file(text: list[str]) -> Generator[str, None, None]:
    with open("file.txt", "w", encoding="utf-8") as file:
        line_count, len_lines = 0, len(text)
        for line in text:
            file.writelines(line + "\n")
            line_count += 1
            yield f"Line {line_count} of {len_lines} write to file"
    yield "All lines write to file."


def read_file() -> Generator[str, None, None]:
    with open("file.txt", "r", encoding="utf-8") as file:
        line_count = 0
        for line in file:
            line_count += 1
            yield f"Get line: {line}"
    yield f"All lines read, line count is {line_count}."
