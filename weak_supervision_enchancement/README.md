# MeSH_retrospective_dataset
## Weak supervision enhancement of the dataset

The enhancement of the dataset is achieved using simpler 
dictionary-based approaches can also be quite strong in some cases with the concept occurrence.
We use the [*Snorkel*](https://www.snorkel.org/), a state-of-the-art tool, in order to combine heuristics into a single label.

The main file of that method is [*snorkel_model.py*](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/weak_supervision_enchancement/snorkel_labeling/snorkel_model.py) and the implementation of the heuristics, as label functions, take place in [*LFs.py*](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/weak_supervision_enchancement/snorkel_labeling/LFs.py). Three approaches are considered:
* ***Majority voting***: a label is assigned to an article, if most of the tagging functions assign that label to that article.
* ***At-least-one***: a label is assigned to an article, if any of the labeling functions assign this label to this article.
* ***Label Model***: a label is assigned to an article, based on a weighted combination of the labeling functions (this is a Snorkel's approach).

The method returns an improved version of the datasets.

For more information check the following study:

Nentidis, A., Chatzopoulos, T., Krithara, A., Tsoumakas, G., & Paliouras, G. (2023). Large-scale fine-grained semantic indexing of biomedical literature based on weakly-supervised deep learning. arXiv preprint. https://arxiv.org/pdf/2301.09350.pdf
