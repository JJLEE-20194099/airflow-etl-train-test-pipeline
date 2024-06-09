import numpy as np
from sklearn.preprocessing import OneHotEncoder

def stack_with_onehot(scores):
    new_scores_l = []
    for score in scores:
        n_score = score
        if len(score.shape) == 1:
            enc = OneHotEncoder()
            enc.fit([[i] for i in range(np.min(score), np.max(score) + 1)])
            n_score = enc.transform(np.expand_dims(score, -1)).toarray()
        new_scores_l.append(n_score)
    scores = np.array(new_scores_l)
    return scores

def mean_max(scores):
    scores = np.mean(scores, 0)
    scores = np.argmax(scores, -1)
    return scores