#!/usr/bin/env python
# coding: utf-8

# In[4]:


from mrjob.job import MRJob


class MRReport(MRJob):
    
    def mapper(self, _, line):
        row = line.split(',')
        yield row[3], [row[0], float(row[4])]
    
    def reducer(self, key, values):
        record = list(zip(*list(values)))
        customer = len(set(record[0]))
        revenue = sum(record[1])
        yield key, (customer, revenue)


if __name__ == '__main__':
     MRReport.run()
