from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRCustomerOrderCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_amount,
                   reducer=self.reducer_count_amount),
            MRStep(mapper=self.mapper_sort_amount,
                   reducer = self.reducer_output_amount)
        ]
                      
    def mapper_get_amount(self, _, line):
        (id, item, amount) = line.split(',')
        yield id, float(amount)
                    
    def reducer_count_amount(self, id, amount):
        yield id, sum(amount)

    def mapper_sort_amount(self, id, count):
        adjustment = 0 + count
        yield str(adjustment)+'%04d'%int(count), id
        #yield '%04d'%int(count), id
        
    def reducer_output_amount(self, count, id):
        for word in id:
            yield count[0:], word
                                    
if __name__ == '__main__':
    MRCustomerOrderCount.run()