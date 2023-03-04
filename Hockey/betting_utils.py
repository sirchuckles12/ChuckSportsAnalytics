def convert_prob_to_american(p):
    p = p * 100
    if p == 100:
        return (-10000)
    if p > 50:
        return ((-p / (1 - p / 100)))
    else:
        return(100 / (p / 100) - 100)
