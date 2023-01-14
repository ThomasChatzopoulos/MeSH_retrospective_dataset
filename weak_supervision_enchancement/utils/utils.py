import csv
import json
import xlsxwriter
import numpy as np
import pandas as pd

from os import path, mkdir
from xlsxwriter.utility import xl_rowcol_to_cell
from ast import literal_eval
from datetime import datetime
from time import time
from scipy.stats import wilcoxon


def createDir(c_path, dir_name):
    """
    Use this function to create a new folder.
    :param c_path:      The parent path; The absolute path-folder in which the folder will be created.
                        type: string
    :param dir_name:    The name of the new folder.
                        type: string
    :return:            The absolute path with the new folder.
                        type: string
    """
    dataset_folder = path.join(c_path + "/", dir_name + "/")
    if not path.exists(dataset_folder):
        mkdir(dataset_folder)
    return dataset_folder


def save_evaluation_report(report_json, target_labels, report_json_file, report_csv_file):
    """
    Save the results of the classification_report() in csv/json file.
    :param report_json:         The classification_report() results with parameter output_dict=True
                                type: json
    :param target_labels:       List of the names of the labels
                                type: (string) list
    :param report_json_file:    The path of the json file to store results
                                type: string
    :param report_csv_file:     The path of the csv file to store results
                                type: string
    :return:                    -
    """
    # open the file in the write mode
    if report_json_file is not None:
        with open(report_json_file, 'w') as outfile:
            json.dump(report_json, outfile)

    if report_csv_file is not None:
        with open(report_csv_file, 'w', newline='', encoding='utf-8') as f:
            # create the csv writer
            writer = csv.writer(f)
            header_row = ["label"] + list(report_json['macro avg'].keys())
            writer.writerow(header_row)
            for row in report_json:
                if "avg" in row:
                    # This is a row like "micro avg", "macro avg" etc
                    content_row = [row] + list(report_json[row].values())
                elif "accuracy" in row:
                    # This is an "accuracy" row shown in binary classification
                    content_row = [row, " ", " "] + [report_json[row]] + [" "]
                else:
                    # This is a label row
                    label_name = row  # for binary cases "0.0" and "0.1" for negative and positive instances
                    if len(target_labels) > 1:
                        label_name = target_labels[int(row)]  # for multilabel cases the index of the label (e.g. "0")
                    content_row = [label_name] + list(report_json[row].values())

                # print(content_row)
                # write a row to the csv file
                writer.writerow(content_row)


def printTimestamp():
    """
    It returns a string of current timestamp like: "2022-09-06 17:50:30".
    :return:    current timestamp with %Y-%m-%d %H:%M:%S format
                type: string
    """
    return datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')


def cell(row, col):
    """
    Exactly the same with the "xl_rowcol_to_cell()" function, but with a smaller name.
    Given the row and the column of a cell it returns the "name" of the cell.
    e.g. (0,0)-->'A1', (0,1)-->'B1'
    :param row: The row of the cell.
    :param col: The column of the cell.
    :return:    The "name" of the cell.
    """
    return xl_rowcol_to_cell(row, col)


def createSummaryReport(files_path, label_functions, year):
    """
    It loads and read report files based on LFs and voters with statistics and creates 2 summary files:
        - the first file contains detailed results both for each descriptor and for the whole year
        - the second file contains results for the whole year based on macro avg of the evaluation_report.
    :param files_path:      The path to search the files (usually the workingPath).
                            type: string
    :param label_functions: List with the names of the label functions.
                            type: (string) list
    :param year:            Τhe year associated with the data
                            type: integer
    :return:                -
    """
    results = {}
    xlsx_row = 2
    xlsx_col = 2

    # load results
    results["label_model"] = list(csv.DictReader(open(f"{files_path}/report_label_model_{str(year)}.csv", "r")))
    results["majority_voter"] = list(csv.DictReader(open(f"{files_path}/report_majority_voter_{str(year)}.csv", "r")))
    results["minority_voter"] = list(csv.DictReader(open(f"{files_path}/report_minority_voter_{str(year)}.csv", "r")))
    if label_functions is not None:
        for lf in label_functions:
            results[lf] = list(csv.DictReader(open(f"{files_path}/report_{lf}_lf_{str(year)}.csv", "r")))

    # create excel file for full summary file
    workbook1 = xlsxwriter.Workbook(f"{files_path}/summary_{year}.xlsx")
    worksheet1 = workbook1.add_worksheet()

    headers_format = workbook1.add_format({'bold': 1, 'border': 1, 'align': 'center', 'font_size': 13})
    left_format = workbook1.add_format({'left': 1})

    # 2 first columns with labels and support
    worksheet1.merge_range(f"{cell(0,0)}:{cell(0,1)}", year, headers_format)
    worksheet1.write(cell(1, 0), 'label', headers_format)
    worksheet1.write(cell(1, 1), 'support', headers_format)

    # TODO not working for years with only 1 descriptor
    for row in results["label_model"]:
        worksheet1.write(cell(xlsx_row, 0), row.get('label'), workbook1.add_format({'bold': 1, 'left': 1}))
        worksheet1.write(cell(xlsx_row, 1), int(row.get('support')), workbook1.add_format({'left': 1}))
        if results["label_model"].index(row) == len(results["label_model"]) - 5 or results["label_model"].index(row) == len(results["label_model"]) - 1:
            worksheet1.conditional_format(f"{cell(xlsx_row, 0)}:{cell(xlsx_row, 1)}", {'type': 'no_blanks', 'format': workbook1.add_format({'bottom': 1, 'left': 1})})

        xlsx_row = xlsx_row + 1

    # fill file for each voter or label function
    for report in results:
        worksheet1.merge_range(f"{cell(0, xlsx_col)}:{cell(0, xlsx_col+2)}", report, headers_format)
        worksheet1.write(cell(1, xlsx_col), 'precision', workbook1.add_format({'bold': 1, 'left': 1, 'bottom': 1, 'align': 'center'}))
        worksheet1.write(cell(1, xlsx_col+1), 'recall', workbook1.add_format({'bold': 1, 'bottom': 1, 'align': 'center'}))
        worksheet1.write(cell(1, xlsx_col+2), 'f1-score', workbook1.add_format({'bold': 1, 'right': 1, 'bottom': 1, 'align': 'center'}))

        xlsx_row = 2
        for row in results[report]:
            worksheet1.write_number(cell(xlsx_row, xlsx_col), float(row.get('precision')), workbook1.add_format({'left': 1}))
            worksheet1.write_number(cell(xlsx_row, xlsx_col+1), float(row.get('recall')))
            worksheet1.write_number(cell(xlsx_row, xlsx_col+2), float(row.get('f1-score')), workbook1.add_format({'right': 1, 'bg_color': 'D9D9D9'}))
            if results[report].index(row) == len(results[report]) - 5 or results[report].index(row) == len(results[report]) - 1:
                worksheet1.conditional_format(f"{cell(xlsx_row, xlsx_col)}", {'type': 'no_blanks', 'format': workbook1.add_format({'bottom': 1, 'left': 1})})
                worksheet1.conditional_format(f"{cell(xlsx_row, xlsx_col+1)}", {'type': 'no_blanks', 'format': workbook1.add_format({'bottom': 1})})
                worksheet1.conditional_format(f"{cell(xlsx_row, xlsx_col+2)}", {'type': 'no_blanks', 'format': workbook1.add_format({'bottom': 1, 'right': 1})})
            if results[report].index(row) == len(results[report]) - 1:
                worksheet1.conditional_format(f"{cell(xlsx_row-3, 0)}:{cell(xlsx_row, xlsx_col+2)}", {'type': 'no_blanks', 'format': workbook1.add_format({'bg_color': 'C6E0B4'})})
                for col in range(4, xlsx_col+2, 3):
                    worksheet1.conditional_format(f"{cell(xlsx_row - 3, col)}:{cell(xlsx_row-2, col)}", {'type': 'no_blanks', 'format': workbook1.add_format({'bg_color': '87BE62'})})
            xlsx_row = xlsx_row + 1
        xlsx_col = xlsx_col + 3

    workbook1.close()

    # create csv file with summary results for macro avg values
    with open(f"{files_path}/short_summary_macro_avg_{year}.csv", 'w', newline='', encoding='utf-8') as f:
        # create the csv writer
        writer = csv.writer(f)
        header_row = ["label function", "precision", "recall", "f1-score", "support"]
        writer.writerow(header_row)
        # for report in results:
        for report in results:
            for row in results[report]:
                if row.get('label') == 'macro avg':
                    content_row = [report, row.get('precision'), row.get('recall'), row.get('f1-score'), row.get('support')]
                    writer.writerow(content_row)


def saveResultsToJSON(data, path, name, year):
    """
    Use this function to save intermediate results from dictionary to JSON file. E.g. lf_results, voter_results.
    This function assumes that each key of the data/dictionary contains an np array, which it converts to a list.
    The file name uses the pattern: {name}_{year}.json.
    :param data:    The dictionary with the data
                    type: Dictionary
    :param path:    The path to save the file
                    type: string
    :param name:    The characteristic name of the file (e.g. lf_results, voter_results)
                    type: string
    :param year:    Τhe year associated with the data
                    type: integer
    :return:        -
    """
    conv_data = {}
    for key in data:
        conv_data[key] = data.get(key).tolist()

    file = open(f"{path}/{name}_{year}.json", "w")
    json.dump(conv_data, file)
    file.close()


def loadResultsFromJSON(path, name, year):
    """
    Load a json file with (temporary) results (e.g. results from voter of label functions) as dictionary.
    The file name uses the pattern: {name}_{year}.json
    :param path:    The path to load the file
                    type: string
    :param name:    The characteristic name of the file (e.g. lf_results, voter_results)
                    type: string
    :param year:    Τhe year associated with the data
                    type: integer
    :return:        Dictionary with the results.
                    type dictionary
    """
    with open(f"{path}/{name}_{year}.json") as json_file:
        data = json.load(json_file)

    conv_data = {}
    for key in data:
        conv_data[key] = np.array(data.get(key))
    return conv_data


def jsonCombFileToCSV(path):
    """
    The json file contains the results of the (n k) combinations of the Label Functions.
    For more information about the content check the file src.weak_labeling.utils.working_with_results.py.
    Each row of the json file concern one combination of the Label Functions. The information retained is about which
    Label Functions have been used, as well the values of f1 micro and scores for the Label Model, the Majority voter
    and the Minority voter.
    This function reorganize all this information so that it is easier to understand for the user and easier to
    manage programmatically and for this a csv file is created.

    :param path:    the working path, where the json file is stored and the csv file will be stored
    :return:        -
    """
    line = 0
    data_dict = {}
    with open(f'{path}/combination_report_2006.json') as json_file:
        file_dict = json.load(json_file)

    for combID in file_dict:
        combIDResults = file_dict[combID]
        newRow = {'combID': combID,
                  'combination': combIDResults[f'label_model_{combID}']['combination'],
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


def LFCombAnalysis(path):
    """
    This function uses the csv file created by the "jsonCombFileToCSV(path)" function of current file.
    This function first locates the maximum f1 score for each case (Label Model, Majority & Minority voter) and the
    number of the LF combinations that have hit that score, and then creates an appropriate csv file.
    The rows of the csv concern the voter cases (label model with f1 micro score, label model with f1 macro score,
    majority voter with f1 micro etc.) and the columns of the csv concern the Label Functions, the maximum score of this
    case and the number of the combinations hit that maximum score.
    More detailed, the columns of the Label Functions reflect how the Label Functions have been distributed based on
    all the combinations that  the maximum score.
    For example: let max_value=0.643, num_of_combinations=5 and synonyms_lowercase=3 for the label_model_f1_micro score.
    The "synonyms_lowercase=3" means that the Label Function "synonyms_lowercase" included in 3 of the 5 combinations.

    :param path:    the working path, where the json file is stored and the csv file will be stored
    :return:        -
    """
    res = {'max_value': [0.0] * 6,
           'num_of_combinations': [0]*6,
           'concept_occurrence_label': [0] *6 ,
           'name_exact': [0] *6,
           'name_exact_lowercase': [0] * 6,
           'name_exact_lowercase_no_punc': [0] *6 ,
           'name_exact_lowercase_tokens_no_punc': [0] *6 ,
           'synonyms_exact': [0] *6 ,
           'synonyms_lowercase': [0] *6 ,
           'synonyms_lowercase_no_punc': [0] *6 ,
           'synonyms_lowercase_tokens_no_punc': [0] *6 ,
           }

    results = pd.DataFrame(res, index=['label_model_f1_micro', 'label_model_f1_macro', 'majority_f1_micro',
                                       'majority_f1_macro', 'minority_f1_micro', 'minority_f1_macro'])
    # results = pd.DataFrame(res, index=['label_model_f1_macro', 'majority_f1_macro', 'minority_f1_macro'])

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


def createOverallSummaryReport(files_path):
    """

    :param files_path:
    :return:
    """
    with open(f"{files_path}/short_summary_macro_avg.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        header_row = ["year",
                      "label_model",
                      "majority_voter",
                      "minority_voter",
                      "concept_occurrence_label",
                      "name_exact_lowercase",
                      "synonyms_lowercase"]
        writer.writerow(header_row)
        for year in range(2006, 2019 + 1, 1):
            fs = pd.read_csv(f"{files_path}/snorkel_results_{year}_0_2_6/short_summary_macro_avg_{year}.csv").to_dict('index')
            content_row = [year,
                           round(fs[0].get('f1-score'), 4),
                           round(fs[1].get('f1-score'), 4),
                           round(fs[2].get('f1-score'), 4),
                           round(fs[3].get('f1-score'), 4),
                           round(fs[4].get('f1-score'), 4),
                           round(fs[5].get('f1-score'), 4)]
            writer.writerow(content_row)


def createOverallDescriptorsSummaryReport(files_path, results_path):
    """

    :param files_path:
    :return:
    """
    with open(f"{results_path}/short_descriptor_summary_macro_avg.csv", 'w', newline='',
              encoding='utf-8') as f:
        writer = csv.writer(f)
        header_row = ["year",
                      "descriptorID",
                      "label_model",
                      "majority_voter",
                      "minority_voter",
                      "concept_occurrence_label",
                      "name_exact_lowercase",
                      "synonyms_lowercase"]
        writer.writerow(header_row)
        for year in range(2007, 2019 + 1, 1):
            descr_ids = pd.read_csv(f"{files_path}/Dataset_SI_old_{year}/UseCasesSelected_" + str(year) + ".csv")["Descr. UI"].values.tolist()
            label_model_report = pd.read_csv(f"{results_path}/snorkel_results_{year}_0_2_6/report_label_model_{year}.csv").set_index('label').to_dict('index')
            majority_report = pd.read_csv(f"{results_path}/snorkel_results_{year}_0_2_6/report_majority_voter_{year}.csv").set_index('label').to_dict('index')
            minority_report = pd.read_csv(f"{results_path}/snorkel_results_{year}_0_2_6/report_minority_voter_{year}.csv").set_index('label').to_dict('index')
            col_report = pd.read_csv(f"{results_path}/snorkel_results_{year}_0_2_6/report_concept_occurrence_label_lf_{year}.csv").set_index('label').to_dict('index')
            name_report = pd.read_csv(f"{results_path}/snorkel_results_{year}_0_2_6/report_name_exact_lowercase_lf_{year}.csv").set_index('label').to_dict('index')
            synon_report = pd.read_csv(f"{results_path}/snorkel_results_{year}_0_2_6/report_synonyms_lowercase_lf_{year}.csv").set_index('label').to_dict('index')

            if len(descr_ids) == 1:
                content_row = [year,
                               descr,
                               round(label_model_report['1'].get('f1-score'), 4),
                               round(majority_report['1'].get('f1-score'), 4),
                               round(minority_report['1'].get('f1-score'), 4),
                               round(col_report['1'].get('f1-score'), 4),
                               round(name_report['1'].get('f1-score'), 4),
                               round(synon_report['1'].get('f1-score'), 4)]
                writer.writerow(content_row)
            else:
                for descr in descr_ids:
                    content_row = [year,
                                   descr,
                                   round(label_model_report[descr].get('f1-score'), 4),
                                   round(majority_report[descr].get('f1-score'), 4),
                                   round(minority_report[descr].get('f1-score'), 4),
                                   round(col_report[descr].get('f1-score'), 4),
                                   round(name_report[descr].get('f1-score'), 4),
                                   round(synon_report[descr].get('f1-score'), 4)]
                    writer.writerow(content_row)


def calcWilcoxonStats(files_path):
    """

    :param files_path:
    :return:
    """
    stats = pd.read_csv(f"{files_path}/short_descriptor_summary_macro_avg.csv")

    print("~~ - ~~ ")
    print("f1-score ", "diff between minority_voter and ", "concept_occurrence_label all")
    d = stats["minority_voter"] - stats["concept_occurrence_label"]
    w, p = wilcoxon(d)
    print("Two-sided Wilcoxon test: H0 \"there is no difference in the two groups\"")
    print("\t W statistic:", w)
    print("\t p-value:", p)
    print("\t p-value (rounded):", round(p, 6))
    w, p = wilcoxon(d, alternative='greater')
    print(
        "Single-sided Wilcoxon test: H0 \"the median of the difference in the two groups is negative\"")
    print("\t W statistic:", w)
    print("\t p-value:", p)
    print("\t p-value (rounded):", round(p, 6))


def papametersAnalysis(filepath):
    line = 0
    data_dict = {}
    with open(f'{filepath}/parameter_grid.json') as json_file:
        file_dict = json.load(json_file)

    for record in file_dict['parameter_grid']:
        params = record['params']
        newRow = {'no': record['no'],
                  'f1-macro': round(record['f1'], 5),
                  'solver': params['solver'],
                  'penalty': params['penalty'],
                  'multi_class': params['multi_class'],
                  'tol': params['tol'],
                  'C': params['C'],
                  'max_iter': params['max_iter']
                  }
        data_dict[line] = newRow
        line += 1

    data = pd.DataFrame.from_dict(data_dict).T
    data.to_csv(f'{filepath}/parameter_grid.csv', index=False)


def newSynonymsFile(filepath):
    data = {}
    synonymsDict = {}
    try:
        data = json.load(open(filepath + "/synonyms.json"))
    except Exception as e:
        print(e)

    for descriptor in data.get("synonyms"):
        synonymsDict.update(descriptor)

    for year in range(2006, 2019 + 1):
        descriptor_names = {}
        UCSelected = pd.read_csv(filepath + "/UseCasesSelected_" + str(year) + ".csv")

        descriptor_IDs = UCSelected["Descr. UI"].values.tolist()
        for d_id in descriptor_IDs:
            for i in range(len(descriptor_IDs)):
                if d_id == UCSelected["Descr. UI"].values.tolist()[i]:
                    synonymsDict[d_id].remove(UCSelected["Descr. Name"].values.tolist()[i])
                    # descriptor_names[d_id] = UCSelected["Descr. Name"].values.tolist()[i]
                    break

    with open(filepath + "/sample.json", "w") as outfile:
        outfile.write('{"synonyms":[\n')
        for d in synonymsDict:
            syns = ' ,'.join(['"' + syn + '"' for syn in synonymsDict[d]])
            if d == 'D000078783':
                line = '{' + '"' + d + '": [' + syns + ']}]}'
            else:
                line = '{' + '"' + d + '": [' + syns + ']},\n'
            outfile.write(line)
