def f(str):
    for i in range(len(str)):
        if str.count(str[i]) != 1: continue
        return i
    return -1

print(f("aabb"))