
from collections import defaultdict


max_val = 150
paths, path2_map = [], defaultdict(list)
with open(f"/Users/sunho/Dropbox/Boston/CS6240/HW1/twitter-dataset/data/edges.csv") as f:
    for line in f.readlines():
        split = line.split(',')
        from_, to_ = int(split[0]), int(split[1].strip())
        if from_ < max_val and to_ < max_val:
            path2_map[from_].append(to_)
    print("generated paths and path2_map")

# for idx in range(6):
#     with open(f"/Users/sunho/Dropbox/Boston/CS6240/HW2/hw-2-jill666666/path2/part-m-0000{idx}") as f:
#         for line in f.readlines():
#             split = line.split(',')
#             a, b, c = int(split[0]), int(split[1]), int(split[2].strip())
#             if b in path2_map[a] and c in path2_map[b] and a in path2_map[c]:
#                 print(f"verified triangle: {a} -> {b} -> {c}")
#             else:
#                 print(f"error::not a triangle: {a} -> {b} -> {c}")
#                 print(path2_map[a])
#                 print(path2_map[b])
#                 print(path2_map[c])


def mapper(paths, path2_map):
    triangle_count = 0
    for path in paths:
        from_, to_ = path[0], path[1]
        for node1 in path2_map[to_]:
            if from_ in path2_map[node1]:
                print('triangle found / edge --',
                      '(', from_, '-->', node1, '-->', to_, ')')
                triangle_count += 1
    return triangle_count / 3


triangle_count = mapper(paths, path2_map)
print(triangle_count)
