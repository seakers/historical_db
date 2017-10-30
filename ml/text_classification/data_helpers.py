import numpy as np
import os
import string
import spacy

nlp = spacy.load('en')

def clean_str(line):
    doc = nlp(line)
    # Pre-process the strings
    tokens = []
    for token in doc:

        # If stopword or punctuation, ignore token and continue
        if (token.is_stop and not (token.lemma_ == "which" or token.lemma_ == "how" or token.lemma_ == "what"
                                   or token.lemma_ == "when" or token.lemma_ == "why")) \
                or token.is_punct:
            continue

        # Lemmatize the token and yield
        tokens.append(token.lemma_)
    return " ".join(tokens)


def batch_iter(data, batch_size, num_epochs, shuffle=True):
    """
    Generates a batch iterator for a dataset.
    """
    data = np.array(data)
    data_size = len(data)
    num_batches_per_epoch = int((len(data)-1)/batch_size) + 1
    for epoch in range(num_epochs):
        # Shuffle the data at each epoch
        if shuffle:
            shuffle_indices = np.random.permutation(np.arange(data_size))
            shuffled_data = data[shuffle_indices]
        else:
            shuffled_data = data
        for batch_num in range(num_batches_per_epoch):
            start_index = batch_num * batch_size
            end_index = min((batch_num + 1) * batch_size, data_size)
            yield shuffled_data[start_index:end_index]


def load_data_and_labels(data_folder):
    # Load all the categories
    general_x_text = []
    general_labels = []
    specific_x_texts = [[], [], [], []]
    specific_labels = [[], [], [], []]

    options_list = ["iFEED", "VASSAR", "Critic", "Historian"]
    num_general_labels = 4
    # Add texts and labels
    num_specific_labels = [0, 0, 0, 0]
    dict_specific_labels = [{}, {}, {}, {}]
    for filename in os.listdir(data_folder):
        specific_label = int(filename.split('.', 1)[0])
        with open(data_folder + filename, 'r') as file:
            file_labels = next(file)[:-1]
            file_labels = [b == "1" for b in file_labels]
            for index in range(num_general_labels):
                if file_labels[index]:
                    dict_specific_labels[index][specific_label] = num_specific_labels[index]
                    num_specific_labels[index] += 1


    for filename in os.listdir(data_folder):
        specific_label = int(filename.split('.', 1)[0])
        with open(data_folder + filename, 'r') as file:
            file_general_labels = next(file)[:-1]
            file_general_labels = [b == "1" for b in file_general_labels]
            for line in file:
                clean_line = clean_str(line)
                # Add to general training
                general_x_text.append(clean_line)
                general_labels.append(file_general_labels)

                # Add to specific models training
                for index in range(num_general_labels):
                    if file_general_labels[index]:
                        specific_x_texts[index].append(clean_line)
                        label_vec = [0 for i in range(num_specific_labels[index])]
                        label_vec[dict_specific_labels[index][specific_label]] = 1
                        specific_labels[index].append(label_vec)

    general_y = np.array(general_labels)
    specific_ys = [[], [], [], []]
    for index in range(num_general_labels):
        specific_ys[index] = np.array(specific_labels[index])
    return [general_x_text, general_y, specific_x_texts, specific_ys]


def load_embedding_vectors_word2vec(vocabulary, filename, binary):
    # load embedding_vectors from the word2vec
    encoding = 'utf-8'
    with open(filename, "rb") as f:
        header = f.readline()
        vocab_size, vector_size = map(int, header.split())
        # initial matrix with random uniform
        embedding_vectors = np.random.uniform(-0.25, 0.25, (len(vocabulary), vector_size))
        if binary:
            binary_len = np.dtype('float32').itemsize * vector_size
            for line_no in range(vocab_size):
                word = []
                while True:
                    ch = f.read(1)
                    if ch == b' ':
                        break
                    if ch == b'':
                        raise EOFError("unexpected end of input; is count incorrect or file otherwise damaged?")
                    if ch != b'\n':
                        word.append(ch)
                word = str(b''.join(word), encoding=encoding, errors='strict')
                idx = vocabulary.get(word)
                if idx != 0:
                    embedding_vectors[idx] = np.fromstring(f.read(binary_len), dtype='float32')
                else:
                    f.seek(binary_len, 1)
        else:
            for line_no in range(vocab_size):
                line = f.readline()
                if line == b'':
                    raise EOFError("unexpected end of input; is count incorrect or file otherwise damaged?")
                parts = str(line.rstrip(), encoding=encoding, errors='strict').split(" ")
                if len(parts) != vector_size + 1:
                    raise ValueError("invalid vector on line %s (is this really the text format?)" % (line_no))
                word, vector = parts[0], list(map('float32', parts[1:]))
                idx = vocabulary.get(word)
                if idx != 0:
                    embedding_vectors[idx] = vector
        f.close()
        return embedding_vectors


def load_embedding_vectors_glove(vocabulary, filename, vector_size):
    # load embedding_vectors from the glove
    # initial matrix with random uniform
    embedding_vectors = np.random.uniform(-0.25, 0.25, (len(vocabulary), vector_size))
    f = open(filename)
    for line in f:
        values = line.split()
        word = values[0]
        vector = np.asarray(values[1:], dtype="float32")
        idx = vocabulary.get(word)
        if idx != 0:
            embedding_vectors[idx] = vector
    f.close()
    return embedding_vectors