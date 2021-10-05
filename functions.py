def search(dictionary, substr, sort=True):

    result = []
    for key in dictionary:
        if substr in key:
            result.append(key)
    if sort: return sorted(result)
    return result


