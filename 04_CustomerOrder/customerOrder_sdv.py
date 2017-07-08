from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRCustomerOrderCount(MRJob):

    def mean(self,amount):
        """Return the sample arithmetic mean of data."""
        n=len(amount)
        return sum(amount)/float(n)

    def _ss(self,amount):
        """Return sum of square deviations of sequence data."""
        c = self.mean(amount)
        ss = sum((x-c)**2 for x in amount)
        return ss
              
    def mapper(self, _, line):
        (id, item, amount) = line.split(',')
        yield id, float(amount)
                    
    def reducer(self, id, amount):
        sss = self._ss(list(amount))
        yield id, sss
            
if __name__ == '__main__':
    MRCustomerOrderCount.run()