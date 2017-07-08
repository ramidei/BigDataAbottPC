from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRCustomerOrderCount(MRJob):
       
    def mapper(self, _, line):
        (id, item, amount) = line.split(',')
        yield id, float(amount)
                    
    def reducer(self, id, amount):
        yield id, sum(amount)
            
if __name__ == '__main__':
    MRCustomerOrderCount.run()