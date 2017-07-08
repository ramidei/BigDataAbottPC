from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   #reducer=self.reducer_count_words),
            #MRStep(mapper=self.mapper_make_counts_key,
                   #reducer = self.reducer_output_words)
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)                   
        ]

    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)

  #  def mapper_make_counts_key(self, word, count):
  #      yield '%04d'%int(count), word

  #  def reducer_output_words(self, count, words):
  #      for word in words:
  #          yield 0-count, word
    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)

if __name__ == '__main__':
    MRWordFrequencyCount.run()