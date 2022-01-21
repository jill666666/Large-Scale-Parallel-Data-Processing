
import re

from os import listdir
from os.path import join

numbers = re.compile(r'\d+(?:\.\d+)?')

implementations = ["RDD-G", "RDD-R", "RDD-F", "RDD-A", "DSET"]
for impl in implementations:
    print("inspecting output directory", impl)
    output_directory = "/Users/sunho/Dropbox/Boston/CS6240/HW3/hw-3-jill666666/outputs/" + impl

    user_count = 0
    for f in listdir(output_directory):
        if f.startswith("p"):
            with open(join(output_directory, f)) as p:
                for line in p.readlines():
                    user_count += 1
                    pairs = numbers.findall(line)
                    user_id, count = int(pairs[0]), int(pairs[1])

                    if user_id == 94289:
                        assert count == 300, "error: id 94289 should have counts 300"
                        print("assertion", user_id, count)
                    else:
                        "unable to find user id 94289"

    print("total count is", user_count, "\n--------------------")
