

def main():

    k = 10000
    initial_pagerank = 1 / (k ** 2)

    with open("input/synthetic.txt", "w") as f:
        for i in range(1, k ** 2 + 1):
            if i % k == 0:
                f.write(f"{i},[],{initial_pagerank}\n")
            else:
                f.write(f"{i},[{i + 1}],{initial_pagerank}\n")


if __name__ == "__main__":
    main()