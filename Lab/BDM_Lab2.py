#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
import sys


# In[15]:


customers = {} # Product ID -> set(string)
revenues = {} # Product ID -> float

with open(sys.argv[1], 'r') as f:
#with open('sale_small.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        #print(row)
        customers[row[3]] = customers.get(row[3], set()).union([row[0]])
        revenues[row[3]] = revenues.get(row[3], 0) + float(row[4])
        #break
        
#customers = [(k,len(v)) for k,v in customers.items()]
#print(customers)

with open(sys.argv[2], 'w') as f:
    writer = csv.writer(f)
    for k,v in customers.items():
        writer.writerow([k,len(v),revenues[k]])

