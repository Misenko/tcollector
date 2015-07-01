def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))
