import os

from datetime import datetime
from shutil import copy
from snorkel.labeling import LFAnalysis
from time import time
from yaml import load, FullLoader

from weak_supervision_enchancement.snorkel_labeling.snorkel_model_functions import (
    loadData,
    getDescriptorsInfo,
    initStatsDF,
    createLabelFunctions,
    applyLFs,
    updateLFsResults,
    updateLFsStatistics,
    applyLabelModel,
    applyMajorityVoter,
    applyMinorityVoter,
    updatePredictions,
    selectKBestLFs,
    calcStatsForVoters,
    calcStatsForLFs,
    saveVoterPredictions,
)
from weak_supervision_enchancement.utils.utils import (
    createSummaryReport,
    saveResultsToJSON,
    createDir,
    printTimestamp,
)

settings = load(open("../../weak_supervision_enchancement/settings.yaml"), Loader=FullLoader)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = settings["TF_CPP_MIN_LOG_LEVEL"]
os.environ["PYTHONHASHSEED"] = settings["PYTHONHASHSEED"]

for year in range(settings["firstYear"], settings["lastYear"] + 1, 1):
    print(f'\n\n\n{printTimestamp()}  **** year: {year}')
    folderName = f"snorkel_results_{year}" + datetime.fromtimestamp(time()).strftime("%Y%m%d%H%M%S")
    workingPath = createDir(settings["workingPath"], folderName)
    copy("../../weak_labeling/settings.yaml", workingPath)
    LFsToUse = settings["LFsToUse"]

    lf_train_results = {}
    lf_test_results = {}
    X_voter_results = {}
    Y_voter_results = {}
    selected_lfs = [0] * len(LFsToUse)

    filesPath = f"{settings['filesPath']}/Dataset_SI_old_{year}"
    trainData, testData, weaklyTestData = loadData(filesPath, year)
    descrIDs, descrNames, descrSynonyms = getDescriptorsInfo(filesPath, year)

    lfs_train_stat, lfs_test_stat = initStatsDF(LFsToUse, descrIDs)

    for d in descrIDs:
        print(printTimestamp() + " --> Descriptor: " + d)
        # The Label Functions to be used
        LFs = createLabelFunctions(d, descrNames.get(d), descrSynonyms.get(d), LFsToUse)

        # Apply Label Functions (LFs) on datasets
        L_train, L_test = applyLFs(LFs, trainData, weaklyTestData)
        lf_train_results = updateLFsResults(lf_train_results, L_train, LFs)
        lf_test_results = updateLFsResults(lf_test_results, L_test, LFs)

        # Statistics for LFs
        stats_train = LFAnalysis(L=L_train, lfs=LFs).lf_summary()
        stats_test = LFAnalysis(L=L_test, lfs=LFs).lf_summary()
        lfs_train_stat, lfs_test_stat = updateLFsStatistics(
            lfs_train_stat, lfs_test_stat, stats_train, stats_test, descrIDs.index(d)
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

        if settings["findKBestLFs"]:
            Data, selected_lfs = selectKBestLFs(
                L_test,
                (testData[d]).values.tolist(),
                settings["numOfBestLFs"],
                selected_lfs
            )

    saveVoterPredictions(
        X_voter_results, trainData, descrIDs, workingPath, "train", year
    )
    saveVoterPredictions(Y_voter_results, testData, descrIDs, workingPath, "test", year)

    saveResultsToJSON(X_voter_results, workingPath, "voters_X_results", year)
    saveResultsToJSON(Y_voter_results, workingPath, "voters_Y_results", year)

    saveResultsToJSON(lf_train_results, workingPath, "lf_train_results", year)
    saveResultsToJSON(lf_test_results, workingPath, "lf_test_results", year)

    # calculate statistics for voters and LFs and save results
    Y_golden = (testData[descrIDs]).values.tolist()
    calcStatsForVoters(
        Y_voter_results, Y_golden, weaklyTestData, descrIDs, workingPath, year
    )
    calcStatsForLFs(
        LFsToUse, lf_test_results, Y_golden, weaklyTestData, descrIDs, workingPath, year
    )

    # save statistics for LFs like Polarity, Coverage, Overlaps, Conflicts
    lfs_train_stat.to_csv(workingPath + "/lf_statistics_train.csv", index=False)
    lfs_test_stat.to_csv(workingPath + "/lf_statistics_test.csv", index=False)

    # create a summary excel (.xlsx) file
    createSummaryReport(workingPath, LFsToUse, year)
