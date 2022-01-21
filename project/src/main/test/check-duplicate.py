from collections import defaultdict


sample_count = 0
edge_map = defaultdict(list)
for idx in range(6):
    # with open(f"./aws_output_log/graph_sampling/twitter/15iter4workers/sample/part-0000{idx}") as f:
    with open(f"./output/part-0000{idx}") as f:
        for line in f.readlines():
            split = line.split(',')
            node, outlink = int(split[0]), int(split[1].strip())

            if outlink in edge_map[node]:
                print(f"duplicate detected: {node} -> {outlink}")
                exit(0)

            sample_count += 1
            edge_map[node].append(outlink)

print(f"number of nodes: {len(edge_map)}")
print(f"number of edges: {sample_count}")
print(f"min node id: {min(edge_map.keys())}")
print(f"max node id: {max(edge_map.keys())}")