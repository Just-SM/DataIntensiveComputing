from mrjob.job import MRJob, MRStep
import json
import re


class MRChiCalculator(MRJob):
    """mrjob derived class used 
    for chi-square calculation
    
    Keyword arguments:
    MRJob -- MRJoB class defined in mrjob module
    Return: text file with top 75 words by chiq-square by category
    """
    

    def configure_args(self):
        super(MRChiCalculator, self).configure_args()
        self.add_file_arg('--stopwords')

    def init_words_mapper(self):
        """
        preload stopwords
        """
        
        self.categories = dict()

        with open(self.options.stopwords) as f:
            self.stopwords = frozenset([x[:-1] for x in f.readlines()])

    def mid_words_mapper(self, _, line):
        """
        stopwords filtering and counting occurences of words per catgory
        """
        
        temp = json.loads(line)

        res = set([x for x in re.split('\W|\d|_', temp['reviewText'].lower()) if len(x) > 1]) - self.stopwords

        if temp['category'] not in self.categories:
            self.categories[temp['category']] = dict()
            self.categories[temp['category']]['@#reserved#@'] = 1
        else:
            self.categories[temp['category']]['@#reserved#@'] = self.categories[temp['category']]['@#reserved#@'] + 1

        for word in res:
            if word not in self.categories[temp['category']]:
                self.categories[temp['category']][word] = 1
            else:
                self.categories[temp['category']][word] = self.categories[temp['category']][word] + 1
    


    def final_words_mapper(self):
        """ yielding <category, word count dictionary>
        """
        

        for key,val in self.categories.items():
            yield key, val

    
    def reduce_cat_number(self,cat,dicts):
        merged_dict = {}
        for d in dicts:
            for key, value in d.items():
                merged_dict[key] = merged_dict.get(key, 0) + value
        yield None,(cat,merged_dict)


    def reduce_all_cats_mid(self,_,data):
        """counting chi-square
        
        Keyword arguments:
        data -- dictionary storing category stings and word count dictionary
        Return: top 75 terms with their chi-square results
        """
        

        self.glob_dict = dict(data)

        total_set = set()

        for cat,cat_dict in self.glob_dict.items():

            cat_list = []

            for word,number_of_ocur_in_cat in cat_dict.items():

                if word == '@#reserved#@':
                    continue

                a = number_of_ocur_in_cat
                b = 0
                c = cat_dict['@#reserved#@'] - a
                d = 0
                n = cat_dict['@#reserved#@']
                for other_cat, other_cat_dict in self.glob_dict.items():
                    if other_cat == cat:
                        continue
                    if word in other_cat_dict:
                        b += other_cat_dict[word]
                        d += other_cat_dict['@#reserved#@'] - other_cat_dict[word]

                    else:
                        d += other_cat_dict['@#reserved#@']
                    n += other_cat_dict['@#reserved#@']

                if (a + b)*(a + c)*(b+d)*(c+d) != 0:
                    cat_list.append( (word, (n*pow(( (a*d) - (b*c) ),2) ) / ( (a + b)*(a + c)*(b+d)*(c+d)  )  ))
                else:
                    cat_list.append( (word, 999999999999))

            sort_list = sorted(cat_list, key=lambda x: x[1],reverse=True)[:75]

            yield cat, " ".join( [ str(x[0]) + ':' + str(x[1]) for x in sort_list ] )

            total_set.update([x[0] for x in sort_list])

        yield None," ".join(sorted(total_set))


    def steps(self):
            return [
                MRStep(mapper_init=self.init_words_mapper,
                       mapper=self.mid_words_mapper,
                       mapper_final=self.final_words_mapper,
                       reducer=self.reduce_cat_number),
                MRStep(
                       reducer=self.reduce_all_cats_mid,),
            ]    
if __name__ == '__main__':
    MRChiCalculator.run()