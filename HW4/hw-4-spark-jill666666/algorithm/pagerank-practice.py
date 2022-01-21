
from collections import defaultdict

outlink_map = {
    'A': ['B', 'C'],
    'B': ['A', 'D'],
    'C': ['B', 'D'],
    'D': ['E'],
    'E': ['B', 'D']
}

# outlink_map = {
#     'A': ['B', 'C', 'D'],
#     'B': ['A'],
#     'C': ['A'],
#     'D': ['A'],
# }

# outlink_map = {
#     'A': ['B', 'C'],
#     'B': ['C'],
#     'C': ['A'],
#     'D': ['C'],
# }

def create_inlink_map(outlink_map):
    inlink_map = defaultdict(list)
    for page, outlinks in outlink_map.items():
        for outlink in outlinks:
            inlink_map[outlink].append(page)
    return inlink_map

def create_pagerank_map(outlink_map, init_pr_val):
    pagerank_map = {}
    for page in outlink_map.keys():
        pagerank_map[page] = init_pr_val
    return pagerank_map

def compute_pagerank(pagerank_map, inlink_map, outlink_map, alpha, k):

    for _ in range(k):
        new_pr_vals = []
        for page, inlinks in inlink_map.items():
            val_sum = 0
            for inlink in inlinks:
                num_outlinks = len(outlink_map[inlink])
                val_sum += pagerank_map[inlink] / num_outlinks
            updated_val = (1 - alpha) / 5 + (alpha * val_sum)
            new_pr_vals.append((page, updated_val))

        for page, updated_val in new_pr_vals:
            pagerank_map[page] = round(updated_val, 3)

        print(pagerank_map)

def main():
    inlink_map = create_inlink_map(outlink_map)
    pagerank_map = create_pagerank_map(outlink_map, init_pr_val=0.1)

    alpha, k = 0.85, 5
    compute_pagerank(pagerank_map, inlink_map, outlink_map, alpha, k)

    pagerank_sum = 0
    for value in pagerank_map.values():
        pagerank_sum += value
    print(pagerank_sum)

if __name__ == "__main__":
    main()