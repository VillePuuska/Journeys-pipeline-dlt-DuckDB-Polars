def delay_sec(s: str) -> int:
    if s[0] == "-":
        neg = -1
        s = s[11 : len(s) - 5].split("M")
    else:
        neg = 1
        s = s[10 : len(s) - 5].split("M")
    return neg * (60 * int(s[0]) + int(s[1]))


def stop_id(s: str) -> str:
    return s[-4:]
