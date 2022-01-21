
from collections import defaultdict


def mapper(paths):

    map_res = defaultdict(list)
    for path in paths:
        from_, to_ = path[0], path[1]
        map_res[from_].append((to_, 'F'))
        map_res[to_].append((from_, 'T'))
    return map_res


def reducer(graph):

    edges, f_arr, t_arr = [], [], []
    for values in graph.values():
        for tuple_ in values:
            node, direction = tuple_[0], tuple_[1]
            if direction == 'F':
                f_arr.append(node)
            elif direction == 'T':
                t_arr.append(node)

        for f in f_arr:
            for t in t_arr:
                edges.append((f, t))
        f_arr.clear()
        t_arr.clear()
    return edges


def count_triangles(paths, edges):

    triangle_count = 0
    for edge in edges:
        if edge in paths:
            print('triangle form found / edge --', edge)
            triangle_count += 1
    return triangle_count // 3


def main():

    paths = [(1, 2), (1, 3), (1, 4), (2, 6), (3, 4), (3, 5), (5, 1)]

    graph = mapper(paths)

    print('graph --')
    for key, values in graph.items():
        print(key, values)

    edges = reducer(graph)
    print('edges --', edges)

    triangle_count = count_triangles(paths, edges)
    print('num of triangles --', triangle_count)


if __name__ == '__main__':
    main()
