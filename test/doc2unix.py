import os

target_suffix = (".rs", ".sh", ".md", ".proto",
                 ".yml", ".bat", ".yml", "Dockerfile")


# remove \r by suffix and overwrite file


def dir2unix(path):
    for line in os.walk(path):
        for name in line[2]:
            if name.endswith(target_suffix):
                file_path = line[0]+"/"+name
                file_obj = open(file_path, "r")
                content = file_obj.read()
                if "\r" in content:
                    print("file "+file_path+" remove \\r begin")
                    content = content.replace("\r", "")
                    open(file_path, "w").write(content)


dir2unix("../docker")
dir2unix("../src")
dir2unix("../proto")
dir2unix("../docs")
