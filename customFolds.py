from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from itertools import chain
from collections import namedtuple, defaultdict
import numpy as np
# from ..utils import get_rng
from surprise.utils import get_rng

class BalancedKFold():
    """Custom k-folds that attempts to generated folds where both
    users and items are equally balanced.
    """

    def __init__(self, n_splits=5, random_state=None, shuffle=True):

        self.n_splits = n_splits
        self.shuffle = shuffle
        self.random_state = random_state

    def prioritize(self, category1, category2, c1Min, index, userID, itemID):
        # amongst the candidate folds priortized by category1, find the ones
        # that would secondarily benefit from category2 balance. Smallest 
        # fold size is a further tie breaker.
        candidates = [i for i, count in enumerate(category1) if count == c1Min]
        triage = sorted([(category2[i], len(self.fold_indices[i]), i) \
                for i in candidates])
        winningFold = triage[0][2]
        self.fold_indices[winningFold].append(index)
        
        self.uCount[userID][0] -= 1
        self.iCount[itemID][0] -= 1
        self.uCount[userID][1][winningFold] += 1
        self.iCount[itemID][1][winningFold] += 1

    def split(self, data):
        """Generator function to iterate over trainsets and testsets.
        Args:
            data(:obj:`Dataset<surprise.dataset.Dataset>`): The data containing
                ratings that will be divided into trainsets and testsets.
        Yields:
            tuple of (trainset, testset)
        """

        if self.n_splits > len(data.raw_ratings) or self.n_splits < 2:
            raise ValueError('Incorrect value for n_splits={0}. '
                             'Must be >=2 and less than the number '
                             'of ratings'.format(len(data.raw_ratings)))

        # We use indices to avoid shuffling the original data.raw_ratings list.
        indices = np.arange(len(data.raw_ratings))

        if self.shuffle:
            get_rng(self.random_state).shuffle(indices)

        n = self.get_n_folds()
        self.fold_indices = [ [] for n in range(n)]

        self.uCount = defaultdict(lambda: [0, [0]*n])
        for userID in [r[0] for r in data.raw_ratings]:
            self.uCount[userID][0] += 1

        self.iCount = defaultdict(lambda: [0, [0]*n])
        for itemID in [r[1] for r in data.raw_ratings]:
            self.iCount[itemID][0] += 1


        for rating_index in range(indices.size):
            rating = data.raw_ratings[rating_index]
            index = indices[rating_index]

            userID = rating[0]
            uRemaing, uFolds = self.uCount[userID]
            itemID = rating[1]
            iRemaing, iFolds = self.iCount[itemID]
            iMin = min(iFolds)
            uMin = min(uFolds)

            # First, check for the easy case where the rating can be assigned to 
            # a fold that is most behind in both categories, and therefore help
            # balance both at once. This also prioritizes cases of zeros in 
            # both categories.
            sumFolds = [sum(i) for i in zip(uFolds, iFolds)]
            candidates = [i for i, sumMins in enumerate(sumFolds) \
                if sumMins == (uMin + iMin)]
            if candidates:
                # a call to prioritize() ordered either way will work
                self.prioritize(uFolds, iFolds, uMin, index, userID, itemID)
                continue

            # From this point, balancing users or items must be chosen at the 
            # expense of the other category.
            # 1st priority: avoid having folds with zero of any user or item.
            # That could make that item or user less valuable for training or 
            # testing
            uZeros = uFolds.count(0)
            iZeros = iFolds.count(0)

            if uZeros or iZeros:
                # Is just one or the other category affected by zeros?
                if not iZeros:
                    self.prioritize(uFolds, iFolds, 0, index, userID, itemID)
                    continue

                if not uZeros:
                    self.prioritize(iFolds, uFolds, 0, index, userID, itemID)
                    continue

                # At this point, both categories have distinct folds with zero(s).
                # Prioritze based on which has the fewest opportunities left to fill
                # the remaining zeros
                uOpportunities = uRemaing - uZeros
                iOpportunities = iRemaing - iZeros

                if uOpportunities < iOpportunities:
                    self.prioritize(uFolds, iFolds, 0, index, userID, itemID)
                    continue
                if iOpportunities < uOpportunities:
                    self.prioritize(iFolds, uFolds, 0, index, userID, itemID)
                    continue

                # The last possible justification for prioritizing item is if it
                # more imbalanced by a larger maximum in the category
                if max(iFolds) > max(uFolds):
                    self.prioritize(iFolds, uFolds, 0, index, userID, itemID)
                    continue

                # All other things being equal, prioritize balancing users over items.
                self.prioritize(uFolds, iFolds, 0, index, userID, itemID)
                continue


            # There are no zeros or mutual minimums at this point.
            # Prioritize category based on which is most imbalanced.
            uImbalance = max(uFolds) / iMin
            iImbalance = max(iFolds) / uMin

            if uImbalance > iImbalance:
                self.prioritize(uFolds, iFolds, uMin, index, userID, itemID)
                continue
            if iImbalance > uImbalance:
                self.prioritize(iFolds, uFolds, iMin, index, userID, itemID)
                continue

            # The last possible justification for prioritizing item is if it has fewer 
            # instances left to distribute
            if iRemaing < uRemaing:
                self.prioritize(iFolds, uFolds, iMin, index, userID, itemID)
                continue

            # All other things being equal, prioritize user over items.
            self.prioritize(uFolds, iFolds, uMin, index, userID, itemID)

        for fold_i in range(n):
            train_folds = [i for i in range(n)]
            del train_folds[fold_i]
            train_chain = chain.from_iterable(
                [self.fold_indices[i] for i in train_folds])

            raw_trainset = [data.raw_ratings[i] for i in train_chain]
            raw_testset = [data.raw_ratings[i] for i in self.fold_indices[fold_i]]

            trainset = data.construct_trainset(raw_trainset)
            testset = data.construct_testset(raw_testset)
            yield trainset, testset


    def get_n_folds(self):

        return self.n_splits