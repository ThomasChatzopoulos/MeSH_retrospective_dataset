import string
import os
import pandas as pd
from yaml import load, FullLoader

from weak_supervision_enchancement.snorkel_labeling.snorkel_model_functions import filtering


def getLFname(label_function):
    """
    From a specific label function name it keeps the main name.
    For example: "lf_concept_occurrence_label_D050681" --> "concept_occurrence_label".
    :param label_function:  The name of the label function.
                            type: string
    :return:                The main name of the label function.
                            type: string
    """
    name_tokens = label_function.name.split("_")
    name = '_'.join(name_tokens[1:len(name_tokens) - 1])
    return name


def punctuationRemover(text):
    """
    Remove punctuation from a string.
    :param text:    The given text-string.
                    type: string
    :return:        The given text-string without punctuation.
                    type: string
    """
    punctuationCharacters = string.punctuation + ' '
    rp_words = []
    words = text.split()
    for word in words:
        for p in punctuationCharacters:
            word = word.replace(p, " ")
        rp_words.append(word)
    return rp_words


def filterLabelMatrix():
    settings_file = open("../settings.yaml")
    settings = load(settings_file, Loader=FullLoader)

    # Parameters
    workingPath = settings["workingPath"]  # The path where the results will be stored
    filesPath = settings["filesPath"]  # The path where the files, like datasets, are stored
    firstYear = settings["firstYear"]  # The first year to consider in the analysis
    lastYear = settings["lastYear"]  # The last year to consider in the analysis

    os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings["TF_CPP_MIN_LOG_LEVEL"]
    os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]

    for year in range(firstYear, lastYear + 1):
        descr_ids = pd.read_csv(f'{filesPath}/UseCasesSelected_{year}.csv')["Descr. UI"].values.tolist()
        train = pd.read_csv(f'{filesPath}/Dataset_SI_old_{year}/train_{year}.csv')
        weakly_test = pd.read_csv(f'{filesPath}/Dataset_SI_old_{year}/weakly_labelled/test_{year}.csv')

        train_results = pd.read_csv(f'{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_train_minority_voter_{year}.csv')
        test_results = pd.read_csv(f'{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_test_minority_voter_{year}.csv')

        filtered_test_data = filtering(
            test_results[descr_ids].to_numpy(),
            weakly_test,
            descr_ids,
        )
        filtered_df = pd.DataFrame(filtered_test_data, columns=descr_ids)
        filtered_df.insert(0, 'pmid', test_results['pmid'], allow_duplicates=False)
        filtered_df.to_csv(
            f"{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_test_minority_voter_{year}_filtered.csv", index_label=["pmid"], index=False
        )

        filtered_train_data = filtering(
            train_results[descr_ids].to_numpy(),
            train,
            descr_ids,
        )
        filtered_df = pd.DataFrame(filtered_train_data, columns=descr_ids)
        filtered_df.insert(0, 'pmid', train['pmid'], allow_duplicates=False)
        filtered_df.to_csv(
            f"{workingPath}/snorkel_results_{year}_0_2_6/label_matrix_train_minority_voter_{year}_filtered.csv",
            index_label=["pmid"], index=False
        )
