from datetime import date
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        company, price, date = line.split(',')
        try:
            float(price)
        except:
            pass
        else:
            yield company, price

    def reducer(self, company, values):
        l = list(values)
        prev_val = l[0]
        grow = True
        for i in l[1:]:
            if (prev_val > i):
                grow = False
                break
            else:
                prev_val = i
        if (grow):
            yield company, l 

if __name__ == '__main__':
    MRWordFrequencyCount.run()