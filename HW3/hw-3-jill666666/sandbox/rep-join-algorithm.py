
from collections import defaultdict


def setup(paths):

    path2_map = defaultdict(list)
    for path in paths:
        from_1, to_1 = path[0], path[1]
        path2_map[from_1].append(to_1)
    return path2_map

def mapper(paths, path2_map):

    triangle_count = 0
    for path in paths:
        from_, to_ = path[0], path[1]
        # if to_ in path2_map[from_]:
        for node1 in path2_map[to_]:  
            for node2 in path2_map[node1]:
                if node2 == from_:
                    print('triangle found / edge --', '(', from_, '-->', to_, ')')
                    triangle_count += 1
    return triangle_count / 3

def main():

    paths = [(1, 2), (1, 3), (1, 4), (2, 1), (2, 6), (3, 4), (3, 5), (5, 1)]

    path2_map = setup(paths)
    print(path2_map)

    print('graph --')
    for key, val in path2_map.items():
        print('from:', key, '/ value:', val)

    triangle_count = mapper(paths, path2_map)
    print('triangle count --', triangle_count)

if __name__ == '__main__':
    main()