

def reshape(iterable, shapefunc):
    for elt in iterable:
        yield(shapefunc(elt))

def expand(iterable, default=None):
    """
    Iterable should be a list of (index, value) tuples corresponding to
    a sparse vector.
    
    will return an iterable of the dense matrix, with default values
    for the missing. 

    e.g. iter= (1,3), (3,4) => None, 3, None 4
    """
    last_index = -1
    for index, value in iterable:
        for _ in range(index - (last_index+1)):
            yield default
        last_index = index
        yield value


if __name__=='__main__':
    print(list(expand([(1,3), (3,4)])))
