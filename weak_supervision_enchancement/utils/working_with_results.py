import csv
import os
import yaml
import json

import pandas as pd
import numpy as np

from datetime import datetime
from itertools import combinations
from shutil import copy
from time import time
import os.path
from os import path

from weak_supervision_enchancement.snorkel_labeling.snorkel_model_functions import (
    loadData,
    getDescriptorsInfo,
    applyLabelModel,
    applyMajorityVoter,
    applyMinorityVoter,
    updatePredictions,
    filtering,
    classification_report,
    saveVoterPredictions,
    calcStatsForVoters,
    calcStatsForLFs
)
from weak_supervision_enchancement.utils.utils import (
    createDir,
    loadResultsFromJSON,
    saveResultsToJSON,
    createSummaryReport,
    printTimestamp
)


def updateFewPredictions(voter_results, y_pred_maj, y_pred_min):
    """
    Auxiliary function for updating and keeping the predictions of the voters
    for each descriptor.
    :param voter_results:   Dictionary to save the results. It probably also contains
                            results for other descriptors.
                            type: dictionary
    :param y_pred_maj:      The predictions of the Majority Label Voter.
                            type: np.ndarray (numPy array)
    :param y_pred_min:      The predictions of the Minority Voter.
                            type: np.ndarray (numPy array)
    :return:                Dictionary with updated results. Containing np.ndarray arrays.
                            type: dictionary
    """
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


def combRepoJSONtoCSV(path, year):
    with open(f"{path}/combination_report_{year}.json") as json_file:
        data = json.load(json_file)

    with open(
            f"{path}/combination_report_{year}.csv", "w", newline="", encoding="utf-8"
    ) as f:
        writer = csv.writer(f)
        header_row = [
            "Label Functions",
            "LM_micro_f1",
            "LM_macro_f1",
            "Maj_micro_f1",
            "Maj_macro_f1",
            "Min_micro_f1",
            "Min_macro_f1",
        ]
        writer.writerow(header_row)
        for combination in data:
            # write the label functions
            comb_dict = data.get(combination)
            content_row = [comb_dict.get(next(iter(comb_dict))).get("combination")]
            if len(combination) == 3:
                content_row = content_row + ["NaN", "NaN"]
            for voter in comb_dict:
                # write fi micro/macro scores
                content_row = content_row + [
                    comb_dict.get(voter).get("micro_f1"),
                    comb_dict.get(voter).get("macro_f1"),
                ]
            writer.writerow(content_row)


def combAnalysis(path, year):
    line = 0
    data_dict = {}
    # data = pd.DataFrame(columns=['combID', 'combination', 'label_model_f1_micro', 'label_model_f1_macro',
    #                              'majority_f1_micro', 'majority_f1_macro', 'minority_f1_micro', 'minority_f1_macro'])

    with open(f'{path}/combination_report_{year}.json') as json_file:
        file_dict = json.load(json_file)

    for combID in file_dict:
        combIDResults = file_dict[combID]
        if len(combID) == 3:
            newRow = {'combID': combID,
                      'combination': combIDResults[f'majority_voter_{combID}']['combination'],
                      'label_model_f1_micro': "NaN",
                      'label_model_f1_macro': "NaN",
                      'majority_f1_micro': combIDResults[f'majority_voter_{combID}']['micro_f1'],
                      'majority_f1_macro': combIDResults[f'majority_voter_{combID}']['macro_f1'],
                      'minority_f1_micro': combIDResults[f'minority_voter_{combID}']['micro_f1'],
                      'minority_f1_macro': combIDResults[f'minority_voter_{combID}']['macro_f1']}
        else:
            newRow = {'combID': combID,
                      'combination': combIDResults[f'majority_voter_{combID}']['combination'],
                      'label_model_f1_micro': combIDResults[f'label_model_{combID}']['micro_f1'],
                      'label_model_f1_macro': combIDResults[f'label_model_{combID}']['macro_f1'],
                      'majority_f1_micro': combIDResults[f'majority_voter_{combID}']['micro_f1'],
                      'majority_f1_macro': combIDResults[f'majority_voter_{combID}']['macro_f1'],
                      'minority_f1_micro': combIDResults[f'minority_voter_{combID}']['micro_f1'],
                      'minority_f1_macro': combIDResults[f'minority_voter_{combID}']['macro_f1']}
        data_dict[line] = newRow
        line += 1

    data = pd.DataFrame.from_dict(data_dict).T
    data.to_csv(f'{path}/combinations.csv', index=False)

    from ast import literal_eval

    res = {'max_value': [0.0] * 6,
           'num_of_combinations': [0] * 6,
           'concept_occurrence_label': [0] * 6,
           'name_exact': [0] * 6,
           'name_exact_lowercase': [0] * 6,
           'name_exact_lowercase_no_punc': [0] * 6,
           'name_exact_lowercase_tokens': [0] * 6,
           'synonyms_exact': [0] * 6,
           'synonyms_lowercase': [0] * 6,
           'synonyms_lowercase_no_punc': [0] * 6,
           'synonyms_lowercase_tokens': [0] * 6
           }
    results = pd.DataFrame(res, index=['label_model_f1_micro',
                                       'label_model_f1_macro',
                                       'majority_f1_micro',
                                       'majority_f1_macro',
                                       'minority_f1_micro',
                                       'minority_f1_macro'])

    data = pd.read_csv(f'{path}/combinations.csv')
    modelMetrics = list(data.columns)[-6:]

    for modelMetric in modelMetrics:
        maxValue = data[modelMetric].max()
        max_indexes = data.index[data[modelMetric] == maxValue].tolist()
        for i in max_indexes:
            results.at[modelMetric, 'max_value'] = maxValue
            results.at[modelMetric, 'num_of_combinations'] = len(max_indexes)
            comb = literal_eval(data.iloc[i]['combination'])
            for c in comb:
                results.at[modelMetric, c] = results.loc[modelMetric][c] + 1

    data = pd.DataFrame.from_dict(results)
    data.to_csv(f'{path}/combinations_analysis.csv')


######################################
#                                    #
#     for (n k) combinations         #
#                                    #
######################################


def createLabelMatrixFromResults(lf_results, lfs_to_use, index):
    label_matrix = lf_results.get(lfs_to_use[0])[index]
    for lf in lfs_to_use[1:]:
        label_matrix = np.vstack((label_matrix, lf_results.get(lf)[index]))

    return np.transpose(label_matrix)


def getCombinationsOfLFs(low, lfs):
    """
    Get a list with combinations of LFs containing at least 3 LFs.
    (n k) = n!/(k!(n-k)!), where n -> lfs, k -> in range(3, len(lfs))
    :param low:
    :param lfs: List with the LFs.
                type: (string) List
    :return:    List containing lists with combinations
    """
    com = []
    for lf in range(low, len(lfs)+1):
        for combination in list(combinations(lfs, lf)):
            com.append(list(combination))
    return com


def getCombID(lfs_to_use, lfs_comb):
    """
    Return the positions of the LFs of the lfs_comb in the lfs_to_use joined with '_'.
    Using this function you create a unique id for each lf combination.
    e.g.    lfs_to_use = ["occurrence_label", "exact", "synonyms"]
            lfs_comb = ["occurrence_label", "synonyms"]
            then CombID = "0_2"
    :param lfs_to_use:  List with all the label function names
                        type: (string) list
    :param lfs_comb:    List with the label function names of the combination
                        type: (string) list
    :return:            combination id
                        string
    """
    position = []
    for lf in lfs_comb:
        position.append(str(lfs_to_use.index(lf)))
    return "_".join(position)


def allPossibleNKComb():
    settings_file = open("../settings.yaml")
    settings = yaml.load(settings_file, Loader=yaml.FullLoader)

    # Parameters
    workingPath = settings["workingPath"]  # The path where the results will be stored
    filesPath = (
        f'{settings["filesPath"]}'  # The path where the files, like datasets, are stored
    )
    firstYear = settings["firstYear"]  # The first year to consider in the analysis
    lastYear = settings["lastYear"]  # The last year to consider in the analysis
    LFsToUse = settings["LFsToUse"]  # A list with the LFs you want to apply

    os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings[
        "TF_CPP_MIN_LOG_LEVEL"
    ]  # Turn off TensorFlow logging messages
    os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]  # For reproducibility

    workingPath = f'{workingPath}/snorkel_results_2006'
    newWorkingPath = (
        workingPath
        + f'/working_with_results_{datetime.fromtimestamp(time()).strftime("%Y%m%d%H%M%S")}/'
    )
    os.mkdir(newWorkingPath)

    for year in range(firstYear, lastYear + 1):
        filesPath = f"{settings['filesPath']}/Dataset_SI_old_{year}"
        lf_train_results = loadResultsFromJSON(workingPath, "lf_train_results", year)
        lf_test_results = loadResultsFromJSON(workingPath, "lf_test_results", year)

        # load data
        trainData, testData, weaklyTestData = loadData(filesPath, year)
        # get information about the descriptors
        descriptorIDs, descriptorNames, descriptorSynonyms = getDescriptorsInfo(
            filesPath, year
        )

        file = open(f"{newWorkingPath}/combination_report_{year}.json", "a")
        file.write("{")
        file.close()

        combs = getCombinationsOfLFs(2, LFsToUse)

        for LFsComb in combs:
            if len(LFsComb) == 2:
                combID = getCombID(LFsToUse, LFsComb)
                print(
                    printTimestamp() + f" ----> combination #{combs.index(LFsComb)} - {combID}"
                )
                voter_results = {}
                for d in descriptorIDs:
                    print(printTimestamp() + " --> Descriptor: " + d)
                    # The Label Functions to be used
                    L_train = createLabelMatrixFromResults(
                        lf_train_results, LFsComb, descriptorIDs.index(d)
                    )
                    L_test = createLabelMatrixFromResults(
                        lf_test_results, LFsComb, descriptorIDs.index(d)
                    )
                    # Apply Voters
                    # X_pred_lm, Y_pred_lm = applyLabelModel(L_train, L_test, False)
                    X_pred_maj, Y_pred_maj = applyMajorityVoter(L_train, L_test)
                    X_pred_min, Y_pred_min = applyMinorityVoter(L_train, L_test)
                    voter_results = updateFewPredictions(
                        # voter_results, Y_pred_lm, Y_pred_maj, Y_pred_min
                        voter_results, Y_pred_maj, Y_pred_min
                    )

                # calculate statistics for voters and LFs and save results
                Y_golden = (testData[descriptorIDs]).values.tolist()

                voter_report = {}
                for voter in voter_results:
                    metrics = {}
                    print(printTimestamp() + f" --> Statistics for {voter}")
                    filtered_data = filtering(
                        np.transpose(np.array(voter_results.get(voter))).tolist(),
                        weaklyTestData,
                        descriptorIDs,
                    )
                    report = classification_report(Y_golden, filtered_data, output_dict=True)
                    metrics["combination"] = LFsComb
                    metrics["micro_f1"] = report.get("micro avg").get("f1-score")
                    metrics["macro_f1"] = report.get("macro avg").get("f1-score")
                    voter_report[f"{voter}_{combID}"] = metrics

                print(voter_report)

            file = open(f"{newWorkingPath}/combination_report_{year}.json", "a")
            file.write(f'"{combID}": ')
            json.dump(voter_report, file)
            file.write(",\n")
            file.close()
        file = open(f"{newWorkingPath}/combination_report_{year}.json", "rb+")
        file.seek(-1, os.SEEK_END)
        file.truncate()
        file.close()
        file = open(f"{newWorkingPath}/combination_report_{year}.json", "a")
        file.write("}")
        file.close()


######################################
#                                    #
#     for specific combination       #
#                                    #
######################################
def specificComb():
    settings_file = open("../settings.yaml")
    settings = yaml.load(settings_file, Loader=yaml.FullLoader)

    # Parameters
    workingPath = settings["workingPath"]  # The path where the results will be stored
    filesPath = settings["filesPath"]  # The path where the files, like datasets, are stored
    firstYear = settings["firstYear"]  # The first year to consider in the analysis
    lastYear = settings["lastYear"]  # The last year to consider in the analysis
    LFsToUse = settings["LFsToUse"]  # A list with the LFs you want to apply

    os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings[
        "TF_CPP_MIN_LOG_LEVEL"
    ]  # Turn off TensorFlow logging messages
    os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]  # For reproducibility

    newWorkingPath = createDir(settings["workingPath"], "snorkel_results_0_2_6")


    for year in range(firstYear, lastYear + 1):
        X_voter_results = {}
        Y_voter_results = {}
        selected_lfs = [0] * len(LFsToUse)

        lf_train_results = loadResultsFromJSON(workingPath, "lf_train_results", year)
        lf_test_results = loadResultsFromJSON(workingPath, "lf_test_results", year)

        # load data
        print("Loading data")
        trainData, testData, weaklyTestData = loadData(filesPath, year)
        # get information about the descriptors
        print("get information about the descriptors")
        descriptorIDs, descriptorNames, descriptorSynonyms = getDescriptorsInfo(
            filesPath, year
        )
        col_names = ["lf", "j", "Polarity", "Coverage", "Overlaps", "Conflicts"]
        lfs_train_stat = pd.DataFrame(
            index=range(len(len(LFsToUse) * descriptorIDs)), columns=col_names
        )
        lfs_test_stat = pd.DataFrame(
            index=range(len(len(LFsToUse) * descriptorIDs)), columns=col_names
        )

        voter_results = {}
        for d in descriptorIDs:
            print(printTimestamp() + " --> Descriptor: " + d)
            # The Label Functions to be used
            L_train = createLabelMatrixFromResults(
                lf_train_results, LFsToUse, descriptorIDs.index(d)
            )
            L_test = createLabelMatrixFromResults(
                lf_test_results, LFsToUse, descriptorIDs.index(d)
            )
            # Apply Voters
            X_pred_lm, Y_pred_lm = applyLabelModel(L_train, L_test, False)
            X_pred_maj, Y_pred_maj = applyMajorityVoter(L_train, L_test)
            X_pred_min, Y_pred_min = applyMinorityVoter(L_train, L_test)
            X_voter_results = updatePredictions(
                X_voter_results, X_pred_lm, X_pred_maj, X_pred_min
            )
            Y_voter_results = updatePredictions(
                Y_voter_results, Y_pred_lm, Y_pred_maj, Y_pred_min
            )

            # Data, selected_lfs = selectKBestLFs(L_test, (testData[d]).values.tolist(), 5, selected_lfs)

        # print(LFsToUse)
        # print(selected_lfs)

        saveVoterPredictions(
            X_voter_results, trainData, descriptorIDs, newWorkingPath, "train", year
        )
        saveVoterPredictions(
            Y_voter_results, testData, descriptorIDs, newWorkingPath, "test", year
        )

        saveResultsToJSON(X_voter_results, newWorkingPath, "voters_X_results", year)
        saveResultsToJSON(Y_voter_results, newWorkingPath, "voters_Y_results", year)

        saveResultsToJSON(lf_train_results, newWorkingPath, "lf_train_results", year)
        saveResultsToJSON(lf_test_results, newWorkingPath, "lf_test_results", year)

        # calculate statistics for voters and LFs and save results

        Y_golden = (testData[descriptorIDs]).values.tolist()
        calcStatsForVoters(
            Y_voter_results, Y_golden, weaklyTestData, descriptorIDs, newWorkingPath, year
        )
        calcStatsForLFs(
            LFsToUse,
            lf_test_results,
            Y_golden,
            weaklyTestData,
            descriptorIDs,
            newWorkingPath,
            year,
        )

        # save statistics for LFs like Polarity, Coverage, Overlaps, Conflicts
        lfs_train_stat.to_csv(newWorkingPath + "/lf_statistics_train.csv", index=False)
        lfs_test_stat.to_csv(newWorkingPath + "/lf_statistics_test.csv", index=False)

        # create a summary excel (.xlsx) file
        createSummaryReport(newWorkingPath, LFsToUse, year)


def filterLabelMatrix():
    settings_file = open("../settings.yaml")
    settings = yaml.load(settings_file, Loader=yaml.FullLoader)

    # Parameters
    workingPath = settings["workingPath"]  # The path where the results will be stored
    filesPath = settings[
        "filesPath"]  # The path where the files, like datasets, are stored
    firstYear = settings["firstYear"]  # The first year to consider in the analysis
    lastYear = settings["lastYear"]  # The last year to consider in the analysis
    LFsToUse = settings["LFsToUse"]  # A list with the LFs you want to apply

    os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings[
        "TF_CPP_MIN_LOG_LEVEL"
    ]  # Turn off TensorFlow logging messages
    os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]  # For reproducibility

    voters = ["label_model", "majority_voter", "minority_voter"]
    for year in range(firstYear, lastYear + 1):
        descr_ids = pd.read_csv(f'{filesPath}/UseCasesSelected_{year}.csv')["Descr. UI"].values.tolist()
        train = pd.read_csv(f'{filesPath}/Dataset_SI_old_{year}/train_{year}.csv')
        weakly_test = pd.read_csv(f'{filesPath}/Dataset_SI_old_{year}/weakly_labelled/test_{year}.csv')
        for voter in voters:
            train_results = pd.read_csv(f'{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_train_{voter}_{year}.csv')
            test_results = pd.read_csv(f'{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_test_{voter}_{year}.csv')

            filtered_test_data = filtering(
                test_results[descr_ids].to_numpy(),
                weakly_test,
                descr_ids,
            )
            filtered_df = pd.DataFrame(filtered_test_data, columns=descr_ids)
            filtered_df.insert(0, 'pmid', test_results['pmid'])
            filtered_df.to_csv(
                f"{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_test_{voter}_{year}_filtered.csv", index_label=["pmid"], index=False
            )

            filtered_train_data = filtering(
                train_results[descr_ids].to_numpy(),
                train,
                descr_ids,
            )
            filtered_df = pd.DataFrame(filtered_train_data, columns=descr_ids)
            filtered_df.insert(0, 'pmid', train['pmid'])
            filtered_df.to_csv(
                f"{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_train_{voter}_{year}_filtered.csv",
                index_label=["pmid"], index=False
            )


def filterLF(lf_name):
    settings_file = open("../settings.yaml")
    settings = yaml.load(settings_file, Loader=yaml.FullLoader)

    # Parameters
    workingPath = settings["workingPath"]  # The path where the results will be stored
    filesPath = settings["filesPath"]  # The path where the files, like datasets, are stored
    firstYear = settings["firstYear"]  # The first year to consider in the analysis
    lastYear = settings["lastYear"]  # The last year to consider in the analysis

    os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings[
        "TF_CPP_MIN_LOG_LEVEL"
    ]  # Turn off TensorFlow logging messages
    os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]  # For reproducibility

    for year in range(firstYear, lastYear + 1):
        print(year)
        descr_ids = pd.read_csv(f'{filesPath}/UseCasesSelected_{year}.csv')["Descr. UI"].values.tolist()
        weakly_test = pd.read_csv(f'{filesPath}/Dataset_SI_old_{year}/weakly_labelled/test_{year}.csv')
        lf_test_results = loadResultsFromJSON(f'{workingPath}/snorkel_results_{year}_0_2_6', "lf_test_results", year)
        print("filter")
        filtered_test_data = filtering(
            lf_test_results[lf_name].transpose(),
            weakly_test,
            descr_ids,
        )
        print("save")
        filtered_df = pd.DataFrame(filtered_test_data, columns=descr_ids)
        filtered_df.insert(0, 'pmid', weakly_test['pmid'])
        filtered_df.to_csv(
            f"{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_test_{lf_name}_{year}_filtered.csv", index_label=["pmid"], index=False
        )


def differences(year, filesPath, workingPath):
    voters = ["label_model", "majority_voter", "minority_voter"]
    datType = ["train", "test"]
    for voter in voters:
        for t in datType:
            test_results = pd.read_csv(f'{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_{t}_{voter}_{year}.csv')
            test_results_filt = pd.read_csv(f'{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_{t}_{voter}_{year}_filtered.csv')
            test_results.compare(test_results_filt).to_csv(f"{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_{t}_{voter}_{year}_diff.csv")


def short_summary():
    settings_file = open("../settings.yaml")

    # Parameters
    workingPath = settings["workingPath"]  # The path where the results will be stored
    filesPath = settings["filesPath"]  # The path where the files, like datasets, are stored
    firstYear = settings["firstYear"]  # The first year to consider in the analysis
    lastYear = settings["lastYear"]  # The last year to consider in the analysis
    LFsToUse = settings["LFsToUse"]  # A list with the LFs you want to apply

    os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings[
        "TF_CPP_MIN_LOG_LEVEL"
    ]  # Turn off TensorFlow logging messages
    os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]  # For reproducibility

    for year in range(firstYear, lastYear + 1):
        print(year)
        results = {}
        xlsx_row = 2
        xlsx_col = 2
        if not path.exists(f"{workingPath}/snorkel_results_{year}_0_2_6/short_summary_macro_avg_{year}.csv"):
            print("create summary")
            # load results
            results["label_model"] = list(csv.DictReader(open(f"{workingPath}/snorkel_results_{year}_0_2_6/report_label_model_{str(year)}.csv", "r")))
            results["majority_voter"] = list(csv.DictReader(open(f"{workingPath}/snorkel_results_{year}_0_2_6/report_majority_voter_{str(year)}.csv", "r")))
            results["minority_voter"] = list(csv.DictReader(open(f"{workingPath}/snorkel_results_{year}_0_2_6/report_minority_voter_{str(year)}.csv", "r")))
            if LFsToUse is not None:
                for lf in LFsToUse:
                    results[lf] = list(csv.DictReader(open(f"{workingPath}/snorkel_results_{year}_0_2_6/report_{lf}_lf_{str(year)}.csv", "r")))
            with open(f"{workingPath}/snorkel_results_{year}_0_2_6/short_summary_macro_avg_{year}.csv", 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                header_row = ["label function", "precision", "recall", "f1-score", "support"]
                writer.writerow(header_row)
                # for report in results:
                for report in results:
                    for row in results[report]:
                        if row.get('label') == 'macro avg':
                            content_row = [report, row.get('precision'), row.get('recall'), row.get('f1-score'), row.get('support')]
                            writer.writerow(content_row)
