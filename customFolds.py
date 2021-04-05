from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from itertools import chain
from math import ceil, floor
import numpy as np
from surprise.utils import get_rng
from surprise.model_selection import KFold

class JumpStartKFolds(KFold):
    """Custom k-folds that allows the use of a larger data set to 
    jump start predictions on a smaller data set. Only the accuracy of 
    predictions for the smaller data set is used in training. The entire
    larger data set is always used in the training set.
    """

    def __init__(
        self, large_data=None, n_splits=5, random_state=None, shuffle=True):
        if large_data == None:
            raise ValueError('Must provide large_data parameter '
                            'for JumpStartKFolds')

        self.large_data = large_data
        super().__init__(
            n_splits=n_splits, random_state=random_state, shuffle=shuffle)


    def split(self, small_data):
        """docstring
        """
        if small_data.reader.rating_scale != \
            self.large_data.reader.rating_scale:

            raise ValueError('Rating scales of large and small data '
                            'sets must match')


        if self.n_splits > len(small_data.raw_ratings) or self.n_splits < 2:
            raise ValueError('Incorrect value for n_splits={0}. '
                             'Must be >=2 and less than the number '
                             'of ratings in small dataset.'.format(
                                len(data.raw_ratings)))

        # We use indices to avoid shuffling the original data.raw_ratings list.
        small_indices = np.arange(len(small_data.raw_ratings))
        large_indices = np.arange(len(self.large_data.raw_ratings))

        if self.shuffle:
            get_rng(self.random_state).shuffle(small_indices)
            get_rng(self.random_state).shuffle(large_indices)

        large_raw_ratings = [self.large_data.raw_ratings[i] for i in large_indices]

        start, stop = 0, 0
        for fold_i in range(self.n_splits):
            start = stop
            stop += len(small_indices) // self.n_splits
            if fold_i < len(small_indices) % self.n_splits:
                stop += 1

            raw_testset = [small_data.raw_ratings[i] for i in \
                chain(small_indices[:start], small_indices[stop:])]
            raw_trainset = [small_data.raw_ratings[i] for i in \
                small_indices[start:stop]]
            raw_trainset += large_raw_ratings

            trainset = small_data.construct_trainset(raw_trainset)
            testset = small_data.construct_testset(raw_testset)

            yield trainset, testset