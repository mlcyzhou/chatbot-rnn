from convokit import Corpus, download
import bz2
import argparse
import os
import json
import re
import sys

FILE_SUFFIX = ".bz2"
OUTPUT_FILE = "output.bz2"


def main():
    assert sys.version_info >= (3, 3), \
        "Must be run in Python 3.3 or later. You are running {}".format(sys.version)

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dataset', type=str, default='friends-corpus',
                        help='dataset name in convokit library')
    parser.add_argument('--savedir', type=str, default='output/',
                        help='directory to save the output')
    parser.add_argument('--data_cache_size', type=int, default=1e7,
                        help='max number of utterances to cache in memory before flushing')
    parser.add_argument('--output_file_size', type=int, default=2e8,
                        help='max size of each output file (give or take one conversation)')
    parser.add_argument('--min_conversation_length', type=int, default=5,
                        help='conversations must have at least this many comments for inclusion')

    args = parser.parse_args()
    parse_main(args)


def parse_main(args):
    dialog_dict = {}
    output_handler = OutputHandler(os.path.join(args.savedir, OUTPUT_FILE), args.output_file_size)
    done = False
    total_read = 0

    while not done:
        done, i = read_data(args.input_dataset, dialog_dict)
        total_read += i
        write_data(dialog_dict, output_handler, args.min_conversation_length)

    dialog_dict.clear()
    print("\nRead all {:,d} lines from {}.".format(total_read, args.input_dataset))


def read_data(dataset_name, data_dict):
    done = False
    cache_count = 0

    # download dataset from Corpus library
    corpus = Corpus(filename=download(dataset_name))
    corpus.print_summary_stats()

    for convo in corpus.iter_conversations():
        # traverse the conversation content in the order of the dialogue
        paths = convo.get_root_to_leaf_paths()
        longest_path = convo.get_longest_paths()
        for path in longest_path:
            # discard invalid text like ""
            allinOneConv = [(utt.speaker.id, utt.text) for utt in path if utt.text != ""]

        cache_count += len(allinOneConv)
        data_dict[convo.id] = allinOneConv

    else:  # raw_data has been exhausted.
        done = True
    print("\rLoad {:,d} utterances\n".format(cache_count), end='')
    return done, cache_count


# OutputHandler class for writing data to output files
class OutputHandler:
    def __init__(self, path, output_file_size):
        if path.endswith(FILE_SUFFIX):
            path = path[:-len(FILE_SUFFIX)]
        self.base_path = path
        self.output_file_size = output_file_size
        self.file_reference = None

    def write(self, data):
        if self.file_reference is None:
            self._get_current_path()
        self.file_reference.write(data)
        self.current_file_size += len(data)
        if self.current_file_size >= self.output_file_size:
            self.file_reference.close()
            self.file_reference = None

    def _get_current_path(self):
        i = 1
        while True:
            path = "{} {}{}".format(self.base_path, i, FILE_SUFFIX)
            if not os.path.exists(path):
                break
            i += 1
        self.current_path = path
        self.current_file_size = 0

        # create file if not exist
        directory = os.path.dirname(self.current_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        if not os.path.exists(self.current_path):
            with open(self.current_path, 'w'):
                pass

        self.file_reference = bz2.open(self.current_path, mode="wt")


# Function to write data from dialog_dict to output files
def write_data(data_dict, output_file, min_conversation_length):

    cache_count = 0
    for key, value in data_dict.items():
        dialog = value

        output_string = ""
        for name, text in dialog:
            if text == '':   # make sure valid utterance
                continue
            output_string += '> ' + name + ': ' + text + '\n'

        if len(dialog) >= min_conversation_length:
            output_file.write(output_string + '\n')
            cache_count += len(dialog)

    print("\rWrote {:,d} utterances\n".format(cache_count), end='')


if __name__ == '__main__':
	main()