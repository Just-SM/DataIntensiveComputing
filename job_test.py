from mrjob.job import MRJob, MRStep
import json
import re


from datetime import datetime


class MRWordFrequencyCount(MRJob):

    def configure_args(self):
        super(MRWordFrequencyCount, self).configure_args()
        self.add_file_arg('--stopwords')

    def mapper_prep_words(self, _, line):
        temp = json.loads(line)
        stopwords = []
        
        with open(self.options.stopwords) as f:
            stopwords = set([x[:-1] for x in f.readlines()])

        #  using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ 
        res = set([x for x in re.split('\W|\d', temp['reviewText'].lower()) if len(x) > 1]) 
    
        res = res - stopwords

        for word in res:
            yield temp['category'], word
    
    def combiner_count_words_per_cat(self, category, words):
        # sum the words we've seen so far 
        agg_dict = dict()
        for word in words:
            if word in agg_dict:
                agg_dict[word] = agg_dict[word] + 1
            else:
                agg_dict[word] = 1

        yield category, agg_dict
        # yield [x.lower() for x in res if len(x) > 1 ] , 1  

    def reducer_combine_cats(self, category, dicts):
        
        combo_dict = dict()
        for words_dict in dicts:
            for word,val in words_dict.items():
                if word in combo_dict:
                    combo_dict[word] = combo_dict[word] + val
                else:
                    combo_dict[word] = val

        yield category, combo_dict
    
    def combiner_calc_chi(self,category,words_dicts):

        words_dict_list = list(words_dicts)
        assert len(words_dict_list) == 1
        words_dict = words_dict_list[0]

        total_words = sum(list(words_dict.values()))
        number_of_words = len(words_dict)
        expected = total_words / number_of_words
        for word, observed in words_dict.items():
            yield category, (word,(pow((expected -observed ),2 ) ) / expected)

    def reducer_combine_chis(self, category, tuples):

        res = sorted(list(tuples),key = lambda x: x[1],reverse=True)

        yield category, " ".join([ str(x[0])+" : "+ str(x[1]) for x in res[:10]])


    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_prep_words,
                   combiner=self.combiner_count_words_per_cat,
                   reducer=self.reducer_combine_cats),
            MRStep(
                combiner=self.combiner_calc_chi,
                   reducer=self.reducer_combine_chis),
        ]

    
if __name__ == '__main__':
    # start_time = datetime.now()
    MRWordFrequencyCount.run()
    # end_time = datetime.now()
    # elapsed_time = end_time - start_time
    # print ("Time : " + str(elapsed_time))