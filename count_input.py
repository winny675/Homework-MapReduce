from mrjob.job import MRJob
from mrjob.runner import MRJobRunner
from mrjob.step import MRStep
import re

# get word pattern word from input 
WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):
    # define MRStep 
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]
    # define mapper
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    # combine output from mapper
    def combiner(self, word, counts):
        yield (word, sum(counts))

    # reduce output from combiner
    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    input_data = "input.txt"
    mr_job = MRWordFreqCount(args=[input_data])

    with mr_job.make_runner() as runner:
        runner.run()
        results = list(mr_job.parse_output(runner.cat_output()))
        results.sort(key=lambda x: x[1], reverse=True)
        
        with open("output.txt", "w") as f:
            for key, value in results:
                f.write(f"{key} {value}\n")
                print(key, value)