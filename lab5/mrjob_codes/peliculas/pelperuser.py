from audioop import avg
from datetime import date
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        user, movie,rating,genre,date = line.split(',')
        try:
            float(rating)
        except:
            pass
        else:
            yield user, (movie, float(rating))

    def reducer(self, user, values):
        l = list(values)
        num = len(l)
        avg = sum([x[1] for x in l]) / len(l)
        yield user, (num, avg)

if __name__ == '__main__':
    MRWordFrequencyCount.run()