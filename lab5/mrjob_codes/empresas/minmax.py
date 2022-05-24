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
            yield company, (float(price), date)

    def reducer(self, company, values):
        l = list(values)
        menor = min(l, key = lambda x: x[0])
        mayor = max(l, key = lambda x: x[0])
        yield company, (menor, mayor)

if __name__ == '__main__':
    MRWordFrequencyCount.run()