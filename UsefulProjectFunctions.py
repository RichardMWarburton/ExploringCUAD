import numpy as np
import os

def print_class_samples(annotations,labels,n_rnd=5):
    """
    Function to print n_rnd samples from each class in annotations
    """   
    #For each clustered class
    for i in np.unique(labels):
        #Identify labels with index i
        idx = labels == i
        #Output class i to screen
        print('CLASS {}\n'.format(str(i)))
        #For each n_rnd annotations selected, print to screen
        for rnd_annotation in np.random.choice(annotations[idx],n_rnd):
            print(rnd_annotation)
            print('-----','\n')


def write_vector(contract_list,clause_vectorisation,clause_of_interest):
    """
    Function to write list of contracts and respective clause vectorisation to a TSV txt file in
    cwd + '\\BoC Data\\'
    """
    n_contracts = len(contract_list)
    file_pth =  os.getcwd() + '\\BoC Data\\' + clause_of_interest.replace(' ','_') +'.txt'

    with open(file_pth,'w+',encoding ='UTF-8') as outfile:
        for i in range(n_contracts):
            outfile.write(contract_list[i] +'\t' + str(clause_vectorisation[i]) + '\n')