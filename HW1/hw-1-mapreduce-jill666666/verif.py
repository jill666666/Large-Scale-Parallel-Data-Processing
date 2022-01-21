

# counter = 0
# with open("/Users/sunho/Dropbox/Boston/CS6240/HW1/twitter-dataset/data/edges.csv", "r") as f:
#     for line in f.readlines():
#         text1, text2 = line.split(',')
#         text2 = text2.strip()

#         if text2 == '2701051':
#             counter += 1

# print('2701051', counter)

counter = 0
for idx in range(0, 10):
    with open(f"/Users/sunho/Dropbox/Boston/CS6240/HW1/hw-1-spark-jill666666/output/part-0000{idx}") as f:
        for line in f.readlines():
            counter += 1

for idx in range(10, 40):
    with open(f"/Users/sunho/Dropbox/Boston/CS6240/HW1/hw-1-spark-jill666666/output/part-000{idx}") as f:
        for line in f.readlines():
            counter += 1

print(counter)

counter = 0
with open("/Users/sunho/Dropbox/Boston/CS6240/HW1/hw-1-mapreduce-jill666666/output/part-r-00000") as f:
    for line in f.readlines():
        counter += 1

print(counter)