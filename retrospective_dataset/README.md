# MeSH_retrospective_dataset
## Creation of biomedical content datasets

This project concern dataset creation using PubMed and MeSH thesaurus.
This project is an extension of the project *MeSH_Extension<sup>1</sup>*.

As an extension of the MeSH_Extension project, this project for a given range of years compares MeSH versions and return a list of concepts improved to fine-grained descriptors, based on which the creation of the training and test datasets.
The method uses as heuristic the concept occurrence in the title ot abstract of an article, as the articles of the train dataset are weak-labeled. 

The main class of the project is [*MeSHDiffHarvester.java*](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/retrospective_dataset/DiffHarvester/MeSHDiffHarvester.java) 
and the dataset creation takes place in the class [*DatasetCreator.java*](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/retrospective_dataset/DiffHarvester/DatasetCreator.java). 
The project can be divided into 3 parts:

1. Find the list of the fine-grained descriptors (Method: [selectDatasets](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/retrospective_dataset/DiffHarvester/MeSHDiffHarvester.java#L266))
2. Create training and test datasets (Method: [createJSONDatasets](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/retrospective_dataset/DiffHarvester/DatasetCreator.java#L99))
3. Calculate statistics for the datasets (Method: [calculateStatistics](https://github.com/ThomasChatzopoulos/MeSH_retrospective_dataset/blob/main/retrospective_dataset/DiffHarvester/DatasetCreator.java#L400))

The result of the execution of the project is the datasets for the given range of years, with other utility files. In more detail:

|File|Explanation|
|---|---|
|newDescriptorsFull_YYYY.csv|It contains all the changes in the MeSH ontology between the years YYYY-1 and YYYY|
|UseCasesSelected_YYYY.csv|It contains the fine-graind descriptor between the years YYYY-1 and YYYY that complied the restrictions|
|train_YYYY.json|The training dataset for year YYYY|
|test_YYYY.json|The training dataset for year YYYY|
|statistics_train_YYYY.json|The statistics file of the training datasets for the year YYYY|
|statistics_test_YYYY.json|The statistics file of the test datasets for the year YYYY|
|statisticsSumUp.csv|The sum up file for the statistics|
|descriptorsSumUp.csv|That file contains the number of articles per fine-grained descriptors|
|javalog_*timestamp*.txt|The log file of the execution|

<sup>1</sup> https://github.com/tasosnent/MeSH_Extension
