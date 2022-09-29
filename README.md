# MeSH_retrospective_dataset
## Creation of biomedical content datasets

This project concern dataset creation using PubMed and MeSH ontology.
This project is an extension of the project *MeSH_Extension<sup>1</sup>*; the MeSH_Extension project  

As an extension of the MeSH_Extension project, this project for a given range of years compares MeSH ontology and finds a cotegory of fine-grained descriptors and based on them creates the training and test datasets.

The main class of the project is *MeSHDiffHarvester.java* and the tataset creation takes place in the class *DatasetCreation.java*. 
The project can be divided into 3 parts:

1. Find the list of the fine-graind descriptors (Method: selectDatasets())
2. Create training and test datasets (Method: createJSONDatasets())
3. Calculate statistics for the datasets (Method: calculateStatistics())

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
