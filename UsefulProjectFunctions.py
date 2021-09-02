import numpy as np
import pandas as pd
from collections import Counter


def print_samples(annotations,n_rnd=5):
    """
    Function to print n_rnd samples annotations
    """   
    rnd  = np.random.choice(annotations,n_rnd)
    
    print('{} Random samples from the data:\n'.format(n_rnd))
    for sample in rnd:
        print(sample,'\n')
    

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

def label_cluster(features,class_labs,m=3):
    """
    Function to label clusters in analysis with most common unigrams in features
    """

#Count tokens in each class and identify top x
    overall_counts = {}
    label_counts = {}
    tags ={}

    for l in np.unique(class_labs):
        token_count = Counter()
        idx = class_labs == l
        label_annotations = features[idx]
        label_counts[l] = label_annotations.shape[0]

        #For each annotation in the respective label
        for annotation in label_annotations:
            for token in annotation.split(sep=' '):
                if token != '':
                    token_count[token] += 1

        overall_counts[l] = token_count

        counts_df = pd.DataFrame(overall_counts).fillna(0)

        for col in counts_df:
            count_order = np.argsort(counts_df[col])[::-1]
            sorted_tokens = counts_df.iloc[count_order,col]
            non_zero_counts = sorted_tokens[sorted_tokens!=0]

        label_txt = []
        ratios = []
        for j in range(m):
            try:
                label_txt.append(non_zero_counts.index[j])
                ratios.append('{:.3f}'.format(non_zero_counts[j]/label_counts[col]))
            except:
                pass

        tags[l] = {'Class':l,'Class Label':' -- '.join(label_txt),'Token to Annotation Ratio':' -- '.join(ratios)}

    return tags

def cluster_summary(counts):
    """Function to summarize a counts vector of classes"""
    
    print('Minimum cluster contract count is: {}'.format(counts.min()))
    print('Maximum cluster contract count is: {}'.format(counts.max()))
    print('Mean cluster contract count is: {:.2f}'.format(counts.mean()))
    print('Variance of cluster contract count is: {:.2f}'.format(np.std(counts)))