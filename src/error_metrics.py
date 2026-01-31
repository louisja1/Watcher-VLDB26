import numpy as np


def get_average_q_error(l1, l2):
    assert len(l1) == len(l2)
    err = 0.0
    for i in range(len(l1)):
        x = 1 if l1[i] == 0 else l1[i]
        y = 1 if l2[i] == 0 else l2[i]
        if x > y:
            err += x / y
        else:
            err += y / x
    return np.round(err / len(l1), 6)


def get_percentile_q_error(l1, l2, p):
    assert len(l1) == len(l2)
    assert len(l1) > 0
    assert p >= 0 and p <= 100
    errs = []
    for i in range(len(l1)):
        x = 1 if l1[i] == 0 else l1[i]
        y = 1 if l2[i] == 0 else l2[i]
        if x > y:
            err = x / y
        else:
            err = y / x
        errs.append(err)
    errs.sort()
    id = int(np.round(len(errs) * p / 100.)) - 1
    id = min(max(id, 0), len(errs) - 1)
    return np.round(errs[id], 6)
