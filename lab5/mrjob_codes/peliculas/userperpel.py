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
            yield movie, (user, float(rating))

    def reducer(self, movie, values):
        l = list(values)
        num = len(l)
        avg = sum([x[1] for x in l]) / len(l)
        yield movie, (num, avg)

if __name__ == '__main__':
    MRWordFrequencyCount.run()