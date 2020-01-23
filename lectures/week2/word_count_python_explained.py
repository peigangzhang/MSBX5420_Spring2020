# input file path is here
input_data_path = './big_data_intro.txt'

# use python dictionary to save word to count, (key is word, value is count)
word_count_dict = {}

# open the data file to read
with open(input_data_path, 'r') as f:
    # iterate each line in that data file
    for line in f:
        # split the line to word list
        words = line.split()
        # iterate each word
        for word in words:
            # if the word is not inside the dictionary
            if word not in word_count_dict:
                # add that word to the dictionary and set count to 0
                word_count_dict[word] = 0

            # increase the count by 1
            word_count_dict[word] += 1

# iterate all items in the dictionary
for word, count in word_count_dict.items():
    # print the word and count
    print(word, count)
