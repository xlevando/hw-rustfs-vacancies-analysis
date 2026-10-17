import sys


def main():
    current_key = None
    current_sum = 0

    for line in sys.stdin:
        line = line.rstrip("\n")

        if not line:
            continue

        key, separator, value = line.partition("\t")

        if not separator:
            raise ValueError(f"Некорректная запись: {line!r}")

        try:
            value = int(value)
        except ValueError as exc:
            raise ValueError(
                f"Некорректное значение для ключа {key!r}: {value!r}"
            ) from exc

        if key == current_key:
            current_sum += value
        else:
            if current_key is not None:
                sys.stdout.write(f"{current_key}\t{current_sum}\n")

            current_key = key
            current_sum = value

    if current_key is not None:
        sys.stdout.write(f"{current_key}\t{current_sum}\n")


if __name__ == "__main__":
    main()