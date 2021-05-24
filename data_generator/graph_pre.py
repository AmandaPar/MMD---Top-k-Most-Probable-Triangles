"""
This script takes as input a txt file. Every line of this txt, contains 2 nodes. This script by using
the uniform distribution gives a probability that may an edge exist between these 2 nodes.
Authors: Georgitsis Christos, Parpori Amanda
Date: 22/5/2021
"""

import pandas as pd
import numpy as np

data = pd.read_csv('Datasets/youtube.txt', sep="\t")
size = len(data)
give_probability = np.random.uniform(0, 1, size)
data['give_probability'] = give_probability
print(data.head)
data.to_csv('Datasets/youtube.txt', index=False, sep=' ', header=False)
