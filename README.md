# MeSH_retrospective_dataset

This repository includes the implementation of the Retrospective Beyond MeSH (RetroBM) method for the development of large-scale retrospective datasets for fine-grained semantic indexing, as described in the study[1].
This is a part of the work "Deep Beyond MeSH (DBM)", which is a Large-scale investigation of weakly-supervised deep learning for the fine-grained semantic indexing of biomedical literature.
The implementation of the Deep Beyond MeSH (DBM) method is available [**here**](https://github.com/tasosnent/DBM).

In particular, this repository includes:
1. *The dataset development*: The [**dataset**](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/tree/main/retrospective_dataset) creation based on a retrospective scenario, using the concept occurrence in the title or abstract of an article as heuristic. 
2. *The enhancement of the dataset*: The [**enhancement**](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/tree/main/weak_supervision_enchancement) is achieved by combining a number of heuristics, beyond the concept occurrence.

## Reference
[1]: Nentidis, A., Chatzopoulos, T., Krithara, A., Tsoumakas, G., & Paliouras, G. (2023). Large-scale fine-grained semantic indexing of biomedical literature based on weakly-supervised deep learning (arXiv preprint: 2301.09350v1). **https://arxiv.org/pdf/2301.09350.pdf**
