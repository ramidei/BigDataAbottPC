from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRCustomerOrderCount(MRJob):

    def mean(self,amount):
        """Return the sample arithmetic mean of data."""
        #ll=list(amount)
        #n = len(ll)
        #if n < 1:
        #    raise ValueError('mean requires at least one data point')
        #return sum(amount)/float(n) # in Python 2 use sum(data)/float(n)
        return sum(amount)/float(listm) # in Python 2 use sum(data)/float(n)

    def _ss(self,amount):
        """Return sum of square deviations of sequence data."""
        c = self.mean(amount)
        ss = sum((x-c)**2 for x in amount)
        return ss
              
    def mapper(self, _, line):
        (id, item, amount) = line.split(',')
        #yield id, float(amount)
        listm=list()
        for x in amount:
            list.append(x)
        len(listm)
        yield id, float(amount)
                    
    def reducer(self, id, amount):
        sss = self._ss(amount)
        yield id, sss
            
if __name__ == '__main__':
    MRCustomerOrderCount.run()