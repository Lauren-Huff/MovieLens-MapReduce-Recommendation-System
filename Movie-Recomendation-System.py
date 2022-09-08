# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 11:14:30 2022

@author: hufflaur
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
from io import open

class MostPopularMovie(MRJob):
    
    def configure_args(self):
        super(MostPopularMovie, self).configure_args()
        self.add_file_arg('--items', help='Path to u.item')
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_average_rating),
            MRStep(reducer_init=self.reducer_init,
                   reducer=self.reducer_get_movie_name),
            MRStep(reducer=self.reducer_sort)
            ]

    def mapper_get_movies(self, movie, rating):
        (userID, movieID, rating, timestamp) = rating.split('\t')
        yield movieID, float(rating)
    
    def reducer_average_rating(self, movie, rating):
        total = 0
        numElements = 0 
        for x in rating:
            total += x
            numElements += 1
        if numElements >= 150:
            yield movie, (round(total/numElements,2), numElements)
        
    def reducer_init(self):
        self.movieNames = {}
    
        with open("u.ITEM", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]

    def reducer_get_movie_name(self, movie, rating):
            yield tuple(rating), (self.movieNames[movie])

    def reducer_sort(self, rating, movie):
        for movie in movie:
            yield rating, movie

if __name__ == '__main__':
    MostPopularMovie.run()
    
# !python Movie-Recomendation-System.py --items=ml-100k/u.ITEM ml-100k/u.data > Week3Assignment.txt