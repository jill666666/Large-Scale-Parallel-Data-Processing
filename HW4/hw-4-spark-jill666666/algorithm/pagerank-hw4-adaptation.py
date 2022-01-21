from pprint import pprint


def create_outlink_map(k):

    outlink_map = {}
    for i in range(k ** 2):
        num = i + 1
        if num % k == 0:
            outlink_map[num] = [0]
        else:
            outlink_map[num] = [num + 1]

    return outlink_map


def create_inlink_map(outlink_map):

    inlink_map = {}
    for page, outlinks in outlink_map.items():
        for outlink in outlinks:
            if outlink in inlink_map.keys():
                inlink_map[outlink].append(page)
            else:
                inlink_map[outlink] = [page]

    for page in outlink_map.keys():
        if page not in inlink_map.keys():
            inlink_map[page] = []

    return inlink_map


def create_pagerank_map(outlink_map, initial_pagerank):

    pagerank_map = {}
    for page in outlink_map.keys():
        pagerank_map[page] = initial_pagerank
    pagerank_map[0] = 0
    return pagerank_map


def compute_pagerank(pagerank_map, inlink_map, outlink_map, k, alpha, iteration):

    for _ in range(iteration):
        new_pageranks = []
        for curr_page in outlink_map.keys():
            pagerank = (1 - alpha) / (k ** 2)
            dangling_sum = 0

            print(len(inlink_map[curr_page]))
            print('before compute ', pagerank)
            for inlink_page in inlink_map[curr_page]:
                num_outlinks = len(outlink_map[inlink_page])
                pagerank += alpha * pagerank_map[inlink_page] / num_outlinks
                print("outlink", curr_page, "inlink", inlink_page, "num outlinks", num_outlinks, "pagerank", pagerank)

            for dangling_page in inlink_map[0]:
                dangling_sum += alpha * pagerank_map[dangling_page] / (k ** 2)
                print("outlink", curr_page, "dangling", dangling_page, "dangling score", alpha * pagerank_map[dangling_page] / (k ** 2), )
                pagerank += alpha * pagerank_map[dangling_page] / (k ** 2)

            new_pageranks.append((curr_page, pagerank))
            print("dangling sum is", dangling_sum)
            print("curr page", curr_page, "pagerank", pagerank)

        for p, p_r in new_pageranks:
            pagerank_map[p] = p_r

    return pagerank_map


def main():

    """ parameters """
    k = 6
    alpha = 0.85
    iteration = 3
    initial_pagerank = 1 / (k ** 2)
    

    outlink_map = create_outlink_map(k)
    print("-outlinks-")
    pprint(outlink_map)

    inlink_map = create_inlink_map(outlink_map)
    print("-inlinks-")
    pprint(inlink_map)

    pagerank_map = create_pagerank_map(outlink_map, initial_pagerank)
    print("-initial pageranks-")
    pprint(pagerank_map)

    compute_pagerank(pagerank_map, inlink_map,
                     outlink_map, k, alpha, iteration)
    print("-compute pagerank-")
    pprint(pagerank_map)

    pagerank_sum = 0
    for value in pagerank_map.values():
        pagerank_sum += value
    print("-pagerank sum:", pagerank_sum)


if __name__ == "__main__":
    main()
