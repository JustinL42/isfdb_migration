from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from itertools import chain
from collections import namedtuple, defaultdict
import numpy as np
# from ..utils import get_rng
from surprise.utils import get_rng
from surprise.model_selection import KFold

class JumpStartKFolds(KFold):
    """Custom k-folds that allows the use of a larger data set to 
    jump start predictions on a smaller data set. Only the accuracy of 
    predictions for the smaller data set is used in training. The entire
    larger data set is always used in the training set.
    """

    def split(self, smallData, largeData):
        for testset, trainset in super(smallData):
            trainData = trainset.to_df + largeData.raw_ratings
            yeild testset, trainset