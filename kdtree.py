# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 16:39:27 2019

@author: Minh Duc
"""

import numpy as np
import pandas as pd 
from sklearn.neighbors import KDTree
import csv

csv_columns = ['CELL','NEIGHBORS']

csv_file = "E:\cell\cell_5km.csv"

df = pd.read_csv("cell.csv")

cell = list(df['CELL'])

coordinates = list(zip(df['X'], df['Y'], df['Z']))

tree = KDTree(coordinates, leaf_size=2)

for coordinate in coordinates:  
    data = []
    data.append(coordinate)
    ind = tree.query_radius(data, r=5)

    cellList = ""
    for i in ind[0]:
        cellList += cell[i]
        cellList += ","
        
    cellList = cellList[:-1]

    index = coordinates.index(coordinate)

    out = {
            'CELL': cell[index],
            'NEIGHBORS': cellList
            }

    with open(csv_file, 'a', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, delimiter='|', lineterminator='\n', fieldnames=csv_columns)
        writer.writerow(out)


#rng = np.random.RandomState(10)

#X = rng.random_sample((500000, 3))
#Y = rng.random_sample((500000, 3))

#tree = KDTree(X, leaf_size=2)

#print(tree.query_radius(X[:1], r=0.3, count_only=True))

#ind = tree.query_radius(X[:1], r=0.3)

#print(ind[0][1])

#print(len(X))

#print(len(Y))

#print(cellList)
#index = ind[0][0]

#cell[index]
        
        521151
