import json
import numpy as np
import pandas as pd

from sklearn.metrics import classification_report
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel
from snorkel.labeling.model import MajorityLabelVoter

from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import chi2

from weak_supervision_enchancement.snorkel_labeling.utils import getLFname
from weak_supervision_enchancement.utils.utils import save_evaluation_report, printTimestamp
from weak_supervision_enchancement.snorkel_labeling.LFs import (
    make_check_concept_occurrence_label_lf,
    make_check_name_exact_lf,
    make_check_name_exact_lowercase_lf,
    make_check_name_exact_lowercase_no_punc_lf,
    make_check_name_exact_lowercase_tokens_lf,
    make_check_synonyms_exact_lf,
    make_check_synonyms_lowercase_lf,
    make_check_synonyms_lowercase_no_punc_lf,
    make_check_synonyms_lowercase_tokens_lf,
    make_check_name_exact_negation_lf,
    make_check_name_exact_lowercase_negation_lf,
    make_check_name_exact_lowercase_no_punc_negation_lf,
    make_check_name_exact_lowercase_tokens_negation_lf,
    make_check_synonyms_exact_negation_lf,
    make_check_synonyms_lowercase_negation_lf,
    make_check_synonyms_lowercase_no_punc_negation_lf,
    make_check_synonyms_lowercase_tokens_negation_lf,
)


def loadData(path, year):
    """
    Use this function to load the datasets (train, test and weakly_test).
    :param path:    The path where the datasets are stored.
                    type: string
    :param year:    The desired year.
                    type: int
    :return:        Pandas Dataframes with train and test datasets.
                    type: pandas DataFrame
    """
    train = pd.DataFrame()
    test = pd.DataFrame()
    weakly_test = pd.DataFrame()
    try:
        train = pd.read_csv(path + "/train_" + str(year) + ".csv")
        test = pd.read_csv(path + "/test_" + str(year) + ".csv")
        weakly_test = pd.read_csv(path + "/weakly_labelled/test_" + str(year) + ".csv")
    except Exception as e:
        print(e)
    return train, test, weakly_test


def initStatsDF(lfs, descr_ids):
    """

    :param lfs:
    :param descr_ids:
    :return:
    """
    col_names = ["lf", "j", "Polarity", "Coverage", "Overlaps", "Conflicts"]
    train = pd.DataFrame(index=range(len(len(lfs) * descr_ids)), columns=col_names)
    test = pd.DataFrame(index=range(len(len(lfs) * descr_ids)), columns=col_names)
    return train, test


def loadSynonyms(path):
    """
    Load the descriptor's synonyms (synonyms.json file is required).
    :param path:    The path where the synonyms.json file is stored
                    type: string
    :return:        Dictionary: descriptor_id -> list of synonyms
                    (e.g.: "D050498" -> ["Live Births", "Live Birth"])
                    type: Dictionary
    """
    data = {}
    synonyms = {}
    try:
        data = json.load(open(path + "/synonyms.json"))
    except Exception as e:
        print(e)

    for descriptor in data.get("synonyms"):
        synonyms.update(descriptor)
    return synonyms


def getDescriptorsInfo(path, year):
    """
    Use this function to get information about the descriptors:
        descriptor_IDs:         List with the IDs of the descriptors (e.g.: "D050681")
        descriptor_names:       Dictionary with the names of the descriptors
                                (descriptor_UI -> descriptor_name)
                                (e.g.: "D050681" -> "Bottle-Nosed Dolphins")
        descriptor_synonyms:    Dictionary with the synonyms of the descriptors
                                (descriptor_UI -> list of synonyms)
                                (e.g.: "D050498" -> ["Live Births", "Live Birth"])
    :param path:        The path where the synonyms file is stored.
                        type: string
    :param year:        The desired year.
                        type: int
    :return:            descriptor_IDs, descriptor_names, descriptor_synonyms
                        types: list, dictionary, dictionary
    """
    UCSelected = pd.DataFrame()
    descriptor_names = {}

    try:
        UCSelected = pd.read_csv(path + "/UseCasesSelected_" + str(year) + ".csv")
    except Exception as e:
        print(e)

    descriptor_IDs = UCSelected["Descr. UI"].values.tolist()
    for d_id in descriptor_IDs:
        for i in range(len(descriptor_IDs)):
            if d_id == UCSelected["Descr. UI"].values.tolist()[i]:
                descriptor_names[d_id] = UCSelected["Descr. Name"].values.tolist()[i]
                break

    descriptor_synonyms = loadSynonyms(path)

    return descriptor_IDs, descriptor_names, descriptor_synonyms


def createLabelFunctions(
    descriptor_id, descriptor_name, descriptor_synonyms, lfs_to_use
):
    """
    Create label functions for the given descriptor.
    :param descriptor_id:       The descriptor_id (e.g.: "D050681")
                                type: string
    :param descriptor_name      The name of the descriptor
                                type: string
    :param descriptor_synonyms  List with the synonyms of the descriptor
                                type: (string) list
    :param lfs_to_use           List with the names of the LFs we want to use
                                type: (string) list
    :return:                    A list with the label functions
                                type: (label functions) list
    """
    functions = [
        make_check_concept_occurrence_label_lf(descr_id=descriptor_id),
        make_check_name_exact_lf(descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_name_exact_lowercase_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_name_exact_lowercase_no_punc_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_name_exact_lowercase_tokens_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_synonyms_exact_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_synonyms_lowercase_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_synonyms_lowercase_no_punc_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_synonyms_lowercase_tokens_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_name_exact_negation_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_name_exact_lowercase_negation_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_name_exact_lowercase_no_punc_negation_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_name_exact_lowercase_tokens_negation_lf(
            descr_id=descriptor_id, descr_name=descriptor_name),
        make_check_synonyms_exact_negation_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_synonyms_lowercase_negation_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_synonyms_lowercase_no_punc_negation_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
        make_check_synonyms_lowercase_tokens_negation_lf(
            descr_id=descriptor_id, descr_synon=descriptor_synonyms),
    ]

    lfs = []
    for LFtoUse in lfs_to_use:
        for function in functions:
            if function.name == f"lf_{LFtoUse}_{descriptor_id}":
                lfs.append(function)
                break
    return lfs


def applyLFs(lfs, train_data, test_data):
    """
    Apply the Label Functions (LF) to the datasets and return the label matrix.
    The label matrix is a NumPy array (L) with one column for each LF and one row for
    each data point, where L[i, j] is the label that the jth labeling function output
    for the ith data point.
    :param lfs:         List with the names of the LFs
                        (e.g.: check_weak_label_lf, check_exact_lf)
                        type: (string) list
    :param train_data:  The train dataset
                        type: pandas dataframe
    :param test_data:   The test dataset
                        type: pandas dataframe
    :return:            The label matrix
                        type: np.ndarray (numPy array)
    """
    applier = PandasLFApplier(lfs=lfs)
    print(printTimestamp() + " --> Apply LFs to train dataset")
    train_label_matrix = applier.apply(df=train_data)
    print("\n" + printTimestamp() + " --> Apply LFs to test dataset")
    test_label_matrix = applier.apply(df=test_data)
    return train_label_matrix, test_label_matrix


def updateLFsResults(lf_results, label_matrix, lfs):
    """
    Use this function if you want to keep the results of each label function.
    Usually, this function is called after applying LFs to data for a given descriptor
    to get its label matrix.
    :param lf_results:      Dictionary to save the results. It probably also contains
                            results for other descriptors.
                            type: dictionary
    :param label_matrix:    The label matrix resulting from the application of the LFs.
                            type: np.ndarray (numPy array)
    :param lfs:             List with the label functions.
                            type: (label functions) list
    :return:                Dictionary with updated results.
                            type: dictionary
    """
    for lf in lfs:
        function_name = getLFname(lf)
        if function_name in lf_results:
            lf_results[function_name] = np.vstack(
                (lf_results[function_name], label_matrix[:, lfs.index(lf)])
            )
        else:
            lf_results[function_name] = label_matrix[:, lfs.index(lf)]

    return lf_results


def updateLFsStatistics(train_dataframe, test_dataframe, train_stat, test_stat, index):
    """
    Use this function if you want to keep the statistics of the results of each label
    function, like Polarity, Coverage, Overlaps and Conflicts for a descriptor.
    :param train_dataframe: The dataframe to save/update the train statistics
                            type: pandas DataFrame
    :param test_dataframe:  The dataframe to save/update the test statistics
                            type: pandas DataFrame
    :param train_stat:      The statistic results after applying LFs on train data
                            type: pandas DataFrame
    :param test_stat:       The statistic results after applying LFs on test data
                            type: pandas DataFrame
    :param index:           The index of the descriptor in the descriptor ID list.
                            type: integer
    :return:                (Updated) dataframes (train and test) with statistics.
                            type: pandas Dataframe
    """
    for lf in train_stat.index:
        train_dataframe.loc[index, :] = train_stat.loc[lf, :]
        test_dataframe.loc[index, :] = test_stat.loc[lf, :]
        train_dataframe.loc[index, "lf"] = lf
        test_dataframe.loc[index, "lf"] = lf
        index = index + len(train_dataframe) / len(train_stat)
    return train_dataframe, test_dataframe


def applyLabelModel(train_label_matrix, test_label_matrix, use_defaults):
    """
    Apply the Label Model of snorkel on the train and test data.
    :param train_label_matrix:  The label matrix (LM) for the train data (e.g. L_train)
                                type: np.ndarray (numPy array)
    :param test_label_matrix:   The label matrix (LM) for the test data (e.g. L_test)
                                type: np.ndarray (numPy array)
    :param use_defaults:        Boolean parameter for using or not the default values
                                of Label Model
                                type: Boolean
    :return:                    The predictions of the Label Model.
                                type: np.ndarray (numPy array)
    """
    print(printTimestamp() + " --> The Label Model (Voter) is applied")
    label_model = LabelModel(cardinality=2, verbose=True)
    if use_defaults:
        label_model.fit(L_train=train_label_matrix)
    else:
        label_model.fit(
            L_train=train_label_matrix, n_epochs=500, l2=0.005, log_freq=100, seed=123
        )
    X_prediction = label_model.predict(
        L=train_label_matrix, return_probs=False, tie_break_policy="random"
    )
    Y_prediction = label_model.predict(
        L=test_label_matrix, return_probs=False, tie_break_policy="random"
    )
    return X_prediction, Y_prediction


def applyMajorityVoter(train_label_matrix, test_label_matrix):
    """
    Apply the MajorityLabelVoter of snorkel on the train and test data.
    :param train_label_matrix:  The label matrix (LM) for the train data (e.g. L_train)
                                type: np.ndarray (numPy array)
    :param test_label_matrix:   The label matrix (LM) for the test data (e.g. L_test)
                                type: np.ndarray (numPy array)
    :return:                    The predictions of the MajorityLabelVoter.
                                type: np.ndarray (numPy array)
    """
    print(printTimestamp() + " --> The Majority Voter is applied")
    majority_model = MajorityLabelVoter()
    X_prediction = majority_model.predict(
        L=train_label_matrix, return_probs=False, tie_break_policy="random"
    )
    Y_prediction = majority_model.predict(
        L=test_label_matrix, return_probs=False, tie_break_policy="random"
    )
    return X_prediction, Y_prediction


def applyMinorityVoter(train_label_matrix, test_label_matrix):
    """
    Apply the MinorityLabelVoter of snorkel on the train and test data.
    :param train_label_matrix:  The label matrix (LM) for the train data (e.g. L_train)
                                type: np.ndarray (numPy array)
    :param test_label_matrix:   The label matrix (LM) for the test data (e.g. L_test)
                                type: np.ndarray (numPy array)
    :return:                    The predictions of the MajorityLabelVoter.
                                type: np.ndarray (numPy array)
    """
    print(printTimestamp() + " --> The Minority Voter is applied")
    X_prediction = train_label_matrix.max(axis=1)
    Y_prediction = test_label_matrix.max(axis=1)
    return X_prediction, Y_prediction


def updatePredictions(voter_results, y_pred_lm, y_pred_maj, y_pred_min):
    """
    Auxiliary function for updating and keeping the predictions of the voters
    for each descriptor.
    :param voter_results:   Dictionary to save the results. It probably also contains
                            results for other descriptors.
                            type: dictionary
    :param y_pred_lm:       The predictions of the Label Model.
                            type: np.ndarray (numPy array)
    :param y_pred_maj:      The predictions of the Majority Label Voter.
                            type: np.ndarray (numPy array)
    :param y_pred_min:      The predictions of the Minority Voter.
                            type: np.ndarray (numPy array)
    :return:                Dictionary with updated results. Containing np.ndarray arrays.
                            type: dictionary
    """
    if "label_model" in voter_results:
        voter_results["label_model"] = np.vstack(
            (voter_results["label_model"], y_pred_lm)
        )
    else:
        voter_results["label_model"] = y_pred_lm

    if "majority_voter" in voter_results:
        voter_results["majority_voter"] = np.vstack(
            (voter_results["majority_voter"], y_pred_maj)
        )
    else:
        voter_results["majority_voter"] = y_pred_maj

    if "minority_voter" in voter_results:
        voter_results["minority_voter"] = np.vstack(
            (voter_results["minority_voter"], y_pred_min)
        )
    else:
        voter_results["minority_voter"] = y_pred_min

    return voter_results


def filtering(predictions, dataframe, target_labels, **kwargs):
    """
    Correct values that should not be taken into account
    for the calculation of performance.
    :param predictions:     Numpy array with predictions
                            type: np.ndarray (numpy array)
    :param dataframe:       Test dataset
                            type: pandas dataframe
    :param target_labels:   Usually are the descriptor IDs
                            type: (string) list
    :param **kwargs         If **kwargs contains the "descriptorid", the function filter
                            only that descriptor
                            type: string
    :return:                The checked and corrected values
                            type: np.ndarray (numpy array)
    """
    for initial_index, doc in dataframe.iterrows():
        position_index = dataframe.index.get_loc(initial_index)
        valid_doc_labels = doc["valid_labels"].split(" ")
        if kwargs:
            if kwargs["descriptorid"] not in valid_doc_labels:
                if not predictions[position_index] == 0:
                    predictions[position_index] = 0
        else:
            for li, l in enumerate(target_labels):
                if l not in valid_doc_labels:
                    if not predictions[position_index][li] == 0:
                        predictions[position_index][li] = 0
    return predictions


def calcStatsForVoters(voter_results, golden, weakly_test_data, descr_ids, path, year):
    """
    Given the results-predictions of the voters and the golden values,
    this function foreach voter:
        1. applies filtering
        2. calculates the statistics using the classification_report
        3. saves the statistics in a csv file
    Voters are usually the Label Model, the Majority label voter and the minority voter.

    :param voter_results:   Dictionary containing the predictions per voter. The keys of
                            the dictionary are the names of the voter.
                            The value of a key is a np.ndarray (numpy array).
                            type: Dictionary
    :param golden:          The golden values-labels.
                                type: np.ndarray (numpy array)
    :param weakly_test_data:    The test data with the weak label.
                                They are used for the filtering.
                                type: np.ndarray (numpy array)
    :param descr_ids:           List with the descriptor IDs
                                type: (string) list
    :param path:                The path to save the files (usually the workingPath)
                                type: string
    :param year:                Τhe year associated with the results
                                type: integer
    :return:                    -
    """
    for voter in voter_results:
        print(printTimestamp() + f" --> Statistics for {voter}")
        filtered_data = filtering(
            np.transpose(np.array(voter_results.get(voter))).tolist(),
            weakly_test_data,
            descr_ids,
        )
        report = classification_report(golden, filtered_data, output_dict=True)
        save_evaluation_report(
            report, descr_ids, None, path + f"/report_{voter}_{str(year)}.csv"
        )


def calcStatsForLFs(lfs, results, golden, weakly_test_data, descr_ids, path, year):
    """
    Given the results-predictions for each label function and the golden values,
    this function foreach label function (LF):
        1. applies filtering
        2. calculates the statistics using the classification_report
        3. saves the statistics in a csv file

    :param lfs:                 List the names of the LFs
                                type: (string) list
    :param results:             Dictionary containing the predictions per LF. The keys of dictionary are the names of the LFs.
                                The value of a key is a np.ndarray (numpy array).
                                type: Dictionary
    :param golden:              The golden values-labels.
                                type: np.ndarray (numpy array)
    :param weakly_test_data:    The test data with the weak label. They are used for the filtering.
                                type: np.ndarray (numpy array)
    :param descr_ids:           List with the descriptor IDs
                                type: (string) list
    :param path:                The path to save the files (usually the workingPath)
                                type: string
    :param year:                Τhe year associated with the results
                                type: integer
    :return:                    -
    """

    for lf in lfs:
        print(printTimestamp() + f" --> Statistics for {lf}")
        lf_results = results.get(lf)
        filtered_lf_results = filtering(
            np.transpose(np.array(lf_results)).tolist(), weakly_test_data, descr_ids
        )
        report = classification_report(golden, filtered_lf_results, output_dict=True)
        save_evaluation_report(
            report, descr_ids, None, path + "/" + f"report_{lf}_lf_{str(year)}.csv"
        )


def updateSelectedLFs(first_mask, second_mask):
    """
    Merge 2 boolean lists-masks according to the logic of the function 'OR'.
    :param first_mask:  The first list.
                        type: list (integer)
    :param second_mask: The second list.
                        type: list (boolean)
    :return:            The merged mask.
                        type: list (boolean)
    """
    new_mask = first_mask
    for i in range(len(first_mask)):
        if second_mask[i]:
            new_mask[i] = new_mask[i] + 1
    return new_mask


def selectKBestLFs(x, y, num_of_features, selected_lfs):
    """
    Use this function to select the k-best Label Functions (LFs) of snorkel.
    :param x:               The results of the snorkel of each LF. It is an m*n like
                            matrix (M), where m is the number of the test records-articles
                            and n the number of the LFs. The M(i,j) is the prediction of
                            the jth LF for the ith record-article.
                            type:
    :param y:               The golden labels for test data.
                            type:
    :param num_of_features: The number of features seen during fit.
                            type: integer
    :param selected_lfs:
                            type:
    :return:
    """
    selector = SelectKBest(chi2, k=num_of_features)
    selected_data = selector.fit_transform(x, y)
    mask = selector.get_support()
    updated_mask = updateSelectedLFs(selected_lfs, mask)
    return selected_data, updated_mask


def saveVoterPredictions(results, data, descr_ids, path, dt_type, year):
    """
    Save the prediction matrices for each voter into a csv file.
    (e.g. voters: Label Model, Majority Label Voter, Minority Voter)
    :param results:     It contains the prediction matrices for each voter as key.
                        type: Dictionary containing np.ndarray (numPy array)
    :param data:        he test dataset
                        type: pandas Dataframe
    :param descr_ids:   List with descriptors IDs
                        type: (string) list
    :param path:        The path to save the files (usually the workingPath)
                        type: string
    :param dt_type:     The dt_type (dataset type) can be "train" or "test".
                        type: string
    :param year:        Τhe year associated with the results
                        type: integer
    :return:            -
    """
    for voter in results:
        labelMatrix = np.transpose(np.array(results.get(voter)))
        labelMatrix_dataframe = pd.DataFrame(
            labelMatrix, columns=descr_ids, index=data["pmid"].values.tolist()
        )
        labelMatrix_dataframe.to_csv(
            f"{path}/label_matrix_{dt_type}_{voter}_{year}.csv", index_label=["pmid"]
        )
