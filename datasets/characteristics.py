def parse_characteristics(content):
    # todo generate characteristics in python and store it in some normal
    # format
    data = {}
    phases = ["count", "size", "counts", "csv"]
    phase = 0
    for line in content:
        try:
            if phases[phase] == "count":
                cnt = int(line.split()[1])
                data["files_cnt"] = cnt
                phase += 1
                continue
            if phases[phase] == "size":
                if line == "\n":
                    phase += 1
                    continue
                size = line.split()[0]
                data["files_size"] = size
                continue
            if phases[phase] == "counts":
                if line != "\n":
                    cnt, extension = line.split()
                    cnt = int(cnt)
                    if "extensions" not in data:
                        data["extensions"] = {}
                    data["extensions"][extension] = cnt
                else:
                    phase += 1
                continue
            if phases[phase] == "csv":
                if line == "0\n":
                    continue
                else:
                    cnt, file = line.split()
                    cnt = int(cnt)
                    if "csv" not in data:
                        data["csv"] = {}
                    data["csv"][file] = cnt
        except Exception as e:
            print(e)
    return data
