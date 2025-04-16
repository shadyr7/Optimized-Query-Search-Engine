def simple_json_parser(line):
    data = {}
    key = ''
    value = ''
    in_key = False
    in_value = False
    is_string = False
    i = 0

    while i < len(line):
        char = line[i]

        if char == '"':
            if not in_key and not in_value:
                in_key = True
                key = ''
            elif in_key and not in_value:
                in_key = False
                i += 1
                while i < len(line) and line[i] in [' ', ':']:
                    i += 1
                if i < len(line) and line[i] == '"':
                    is_string = True
                    in_value = True
                    value = ''
                else:
                    is_string = False
                    in_value = True
                    value = ''
                    continue
            elif in_value and is_string:
                in_value = False
                data[key] = value
            else:
                pass
        elif in_key:
            key += char
        elif in_value:
            if is_string:
                value += char
            else:
                if char in [',', '}']:
                    try:
                        if '.' in value:
                            data[key] = float(value)
                        else:
                            data[key] = int(value)
                    except:
                        data[key] = value
                    in_value = False
                else:
                    value += char
        i += 1

    return data
