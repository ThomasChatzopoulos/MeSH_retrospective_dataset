from re import finditer
from snorkel.labeling import LabelingFunction
from weak_supervision_enchancement.snorkel_labeling.utils import punctuationRemover


#################################################
#           Working without negation            #
#################################################


def check_concept_occurrence_label_lf(x, descr_id):
    """
    This is a labeling function that checks the concept occurrence in a dataset record.
    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :return:            1   if concept occurrence label = 1
                        0   otherwise
    """
    return 1 if x[descr_id] == 1 else 0


def check_name_exact_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor exists in full
    in text of the dataset record.
    The check is performed only on the exact match.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name exist in full in the text
                        0   otherwise
    """
    return 1 if descr_name in x["text"] else 0


def check_name_exact_lowercase_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor in lowercase
    exists in text of the dataset record in lowercase.
    The check is performed with exact match in lowercase.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name in lowercase exists in the text
                        0   otherwise
    """
    return 1 if descr_name.lower() in x["text"].lower() else 0


def check_name_exact_lowercase_no_punc_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor in lowercase
    and without punctuation exists in text of the dataset record in lowercase.
    The check is performed with exact match in lowercase.

    Example for descriptor name in lowercase and without punctuation:
    "Bottle-Nosed Dolphins" -> "bottle nosed dolphins"

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name in lowercase and without punctuation
                            exists in the text
                        0   otherwise
    """
    name = " ".join(punctuationRemover(descr_name.lower()))
    return 1 if name in x["text"].lower() else 0


def check_name_exact_lowercase_tokens_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor in lowercase,
    without punctuation and separatedinto tokens exists in text of the dataset record.
    The check is performed with exact match in lowercase.

    Example for descriptor name in lowercase and without punctuation:
    "Bottle-Nosed Dolphins" -> ["bottle", "nosed", "dolphins"]

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name in lowercase, without punctuation and
                            separated into tokens exists in the text
                        0   otherwise
    """
    tokens = punctuationRemover(descr_name)
    text = x["text"].lower()
    for token in tokens:
        if token.lower() in text:
            return 1
    return 0


def check_synonyms_exact_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor
    exist in text of the dataset record.
    The check is performed only on the exact match.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms exist in the text
                        0   otherwise
    """
    for synonym in descr_synon:
        if synonym in x["text"]:
            return 1
    return 0


def check_synonyms_lowercase_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor
    in lowercase exist in text of the dataset record in lowercase.
    The check is performed with exact match in lowercase.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms exist in the text
                        0   otherwise
    """
    text = x["text"].lower()
    for synonym in descr_synon:
        if synonym.lower() in text:
            return 1
    return 0


def check_synonyms_lowercase_no_punc_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor in
    lowercase and without punctuation exist in text of the dataset record in lowercase.
    The check is performed with exact match in lowercase.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms exist in the text
                        0   otherwise
    """
    text = x["text"].lower()
    for synonym in descr_synon:
        if " ".join(punctuationRemover(synonym.lower())) in text:
            return 1
    return 0


def check_synonyms_lowercase_tokens_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor
    in lowercase, without punctuation and .
    separated into tokens exist in text of the dataset record in lowercase.
    The check is performed with exact match in lowercase.

    Example:
    "Oncolytic Virus" and ["Oncolytic Viruses", "Virus, Oncolytic", "Viruses, Oncolytic"]
    --> ["oncolytic", "virus", "viruses"]

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms exist in the text
                        0   otherwise
    """
    text = x["text"].lower()
    tokens = list(set(punctuationRemover(" ".join(descr_synon))))
    for token in tokens:
        if token.lower() in text:
            return 1
    return 0


def make_check_concept_occurrence_label_lf(descr_id):
    """
    Call this function to create a new "check_concept_occurrence_label_lf" label function
    using as keyword the descriptor id.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_concept_occurrence_label_{descr_id}",
        f=check_concept_occurrence_label_lf,
        resources=dict(descr_id=descr_id),
    )


def make_check_name_exact_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lf" label function using
    as keyword the descriptor id.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_{descr_id}",
        f=check_name_exact_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_name_exact_lowercase_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lowercase_lf" label function
    using as keyword the descriptor id and the descriptor name.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_lowercase_{descr_id}",
        f=check_name_exact_lowercase_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_name_exact_lowercase_no_punc_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lowercase_no_punc_lf"
    label function using as keyword the descriptor id.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_lowercase_no_punc_{descr_id}",
        f=check_name_exact_lowercase_no_punc_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_name_exact_lowercase_tokens_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lowercase_tokens_lf"
    label function using as keyword the descriptor id and the descriptor name.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_lowercase_tokens_{descr_id}",
        f=check_name_exact_lowercase_tokens_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_synonyms_exact_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_exact_lf" label function using
    as keyword the descriptor id and a (string) list containing the descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_exact_{descr_id}",
        f=check_synonyms_exact_lf,
        resources=dict(descr_synon=descr_synon),
    )


def make_check_synonyms_lowercase_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_lowercase_lf" label function using
    as keyword the descriptor id and a (string) list containing the descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_lowercase_{descr_id}",
        f=check_synonyms_lowercase_lf,
        resources=dict(descr_synon=descr_synon),
    )


def make_check_synonyms_lowercase_no_punc_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_lowercase_no_punc_lf"
    label function using as keyword the descriptor id and a (string) list containing
    the descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_lowercase_no_punc_{descr_id}",
        f=check_synonyms_lowercase_no_punc_lf,
        resources=dict(descr_synon=descr_synon),
    )


def make_check_synonyms_lowercase_tokens_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_lowercase_tokens_lf"
    label function using as keyword the descriptor id and a (string) list containing
    the descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_lowercase_tokens_{descr_id}",
        f=check_synonyms_lowercase_tokens_lf,
        resources=dict(descr_synon=descr_synon),
    )


#################################################
#            Working with negation              #
#################################################


def detect_negation_lf(x, word, lowercase):
    """
    This function checks if a negation is found into the 8 characters before a word or a
    string (usually: the descriptor name or synonym).

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param word:        The word-string based on which the negation searching take place.
                        type: string
    :param lowercase:   If 'True', convert text to lowercase.
                        type: boolean
    :return:            1   if a negation is found in the text before the given word
                        0   otherwise
    """
    negations = ["n't ", "n' t ", " no ", " nor ", "not ", " cannot "]
    positions = []
    text = x["text"]
    if lowercase:
        text = text.lower()
        word = word.lower()

    if word in text:
        for match in finditer(r"\b" + word + r"\b", text):
            positions.append(match.start())
        for position in positions:
            if position == -1 or position == 0:
                continue
            elif position < 8:
                text_to_check = text[:position]
            else:
                text_to_check = text[position - 8: position]

            for negation in negations:
                if negation in text_to_check:
                    return 1
    return 0


def check_name_exact_negation_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor exists in full
    in text of the dataset record and then check for negation before the descriptor's name.
    The check is performed only on the exact match.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name exist in the text without negation
                        0   otherwise
    """
    if descr_name in x["text"]:
        if detect_negation_lf(x, descr_name, False) == 0:
            return 1
    return 0


def check_name_exact_lowercase_negation_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor in lowercase
    exists in text of the dataset record in lowercase and then check for negation exists
    before the descriptor's name.
    The check is performed with exact match in lowercase.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name in lowercase exists in the text
                            without negation
                        0   otherwise
    """
    if descr_name.lower() in x["text"].lower():
        if detect_negation_lf(x, descr_name, True) == 0:
            return 1
    return 0


def check_name_exact_lowercase_no_punc_negation_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor in lowercase
    and without punctuation exists in text of the dataset record in lowercase and then
    check for negation exists before the descriptor's name.
    The check is performed with exact match in lowercase.

    Example for descriptor name in lowercase and without punctuation:
    "Bottle-Nosed Dolphins" -> "bottle nosed dolphins"

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name in lowercase and without punctuation exists
                            in the text without negation
                        0   otherwise
    """
    no_punc_name = " ".join(punctuationRemover(descr_name.lower()))
    if no_punc_name in x["text"].lower():
        if detect_negation_lf(x, no_punc_name, True) == 0:
            return 1
    return 0


def check_name_exact_lowercase_tokens_negation_lf(x, descr_name):
    """
    This is a labeling function that checks if the name of the descriptor in lowercase,
    without punctuation and separated into tokens exists in text of the dataset record
    and then check for negation exists before each token.
    The check is performed with exact match in lowercase.

    Example for descriptor name in lowercase and without punctuation:
    "Bottle-Nosed Dolphins" -> ["bottle", "nosed", "dolphins"]

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            1   if descriptor name in lowercase, without punctuation and
                            separated into tokens exists in the text without negation
                        0   otherwise
    """
    tokens = punctuationRemover(descr_name)
    text = x["text"].lower()
    for token in tokens:
        if token.lower() in text:
            if detect_negation_lf(x, token, True) == 0:
                return 1
    return 0


def check_synonyms_exact_negation_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor exist
    in text of the dataset record and then check for negation exists before each synonym.
    The check is performed only on the exact match.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms exist in the text without negation
                        0   otherwise
    """
    for synonym in descr_synon:
        if synonym in x["text"]:
            if detect_negation_lf(x, synonym, False) == 0:
                return 1
    return 0


def check_synonyms_lowercase_negation_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor in lowercase
    exist in text of the dataset record in lowercase and then check for negation exists
    before each synonym.
    The check is performed with exact match in lowercase.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms in lowercase exist in the text
                            without negation
                        0   otherwise
    """
    for synonym in descr_synon:
        if synonym.lower() in x["text"].lower():
            if detect_negation_lf(x, synonym, True) == 0:
                return 1
    return 0


def check_synonyms_lowercase_no_punc_negation_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor in lowercase
    and without punctuation exist in text of the dataset record in lowercase and then
    check for negation exists before each synonym.
    The check is performed with exact match in lowercase.

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms in lowercase and without punctuation
                            exist in the text without negation
                        0   otherwise
    """
    text = x["text"].lower()
    for synonym in descr_synon:
        synonym_no_punc = " ".join(punctuationRemover(synonym.lower()))
        if synonym_no_punc in text:
            if detect_negation_lf(x, synonym_no_punc, True) == 0:
                return 1
    return 0


def check_synonyms_lowercase_tokens_negation_lf(x, descr_synon):
    """
    This is a labeling function that checks if the synonyms of the descriptor in
    lowercase, without punctuation and separated into tokens exist in text of the dataset
    record in lowercase and then check for negation exists before each synonym.
    The check is performed with exact match in lowercase.

    Example:
    "Oncolytic Virus" and ["Oncolytic Viruses", "Virus, Oncolytic", "Viruses, Oncolytic"]
    --> ["oncolytic", "virus", "viruses"]

    :param x:           Dataset record in Dataframe
                        type: pandas Series
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            1   if descriptor synonyms in lowercase, without punctuation and
                            separated into tokens exist in the text without negation
                        0   otherwise
    """
    text = x["text"].lower()
    tokens = list(set(punctuationRemover(" ".join(descr_synon))))
    for token in tokens:
        if token.lower() in text:
            if detect_negation_lf(x, token, True) == 0:
                return 1
    return 0


def make_check_name_exact_negation_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_negation_lf" label function using
    as keyword the descriptor id.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_negation_{descr_id}",
        f=check_name_exact_negation_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_name_exact_lowercase_negation_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lowercase_negation_lf" label
    function using as keyword the descriptor id and the descriptor name.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_lowercase_negation_{descr_id}",
        f=check_name_exact_lowercase_negation_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_name_exact_lowercase_no_punc_negation_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lowercase_no_punc_negation_lf"
    label function using as keyword the descriptor id.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_lowercase_no_punc_negation_{descr_id}",
        f=check_name_exact_lowercase_no_punc_negation_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_name_exact_lowercase_tokens_negation_lf(descr_id, descr_name):
    """
    Call this function to create a new "check_name_exact_lowercase_tokens_negation_lf"
    label function using as keyword the descriptor id and the descriptor name.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_name:  The descriptor name (e.g.: "Bottle-Nosed Dolphins")
                        type: string
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_name_exact_lowercase_tokens_negation_{descr_id}",
        f=check_name_exact_lowercase_tokens_negation_lf,
        resources=dict(descr_name=descr_name),
    )


def make_check_synonyms_exact_negation_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_exact_negation_lf" label function
    using as keyword the descriptor id and a (string) list containing the descriptor
    synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_exact_negation_{descr_id}",
        f=check_synonyms_exact_negation_lf,
        resources=dict(descr_synon=descr_synon),
    )


def make_check_synonyms_lowercase_negation_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_lowercase_negation_lf" label
    function using as keyword the descriptor id and a (string) list containing the
    descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_lowercase_negation_{descr_id}",
        f=check_synonyms_lowercase_negation_lf,
        resources=dict(descr_synon=descr_synon),
    )


def make_check_synonyms_lowercase_no_punc_negation_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_lowercase_no_punc_negation_lf"
    label function using as keyword the descriptor id and a (string) list containing
    the descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon: List with the synonyms of the descriptor
                        type: (string) list
    :return:            A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_lowercase_no_punc_negation_{descr_id}",
        f=check_synonyms_lowercase_no_punc_negation_lf,
        resources=dict(descr_synon=descr_synon),
    )


def make_check_synonyms_lowercase_tokens_negation_lf(descr_id, descr_synon):
    """
    Call this function to create a new "check_synonyms_lowercase_tokens_negation_lf"
    label function using as keyword the descriptor id and a (string) list containing the
    descriptor synonyms.

    :param descr_id:    The descriptor ID (e.g.: "D050681")
                        type: string
    :param descr_synon:    List with the synonyms of the descriptor
                                type: (string) list
    :return:                    A Label function
    """
    return LabelingFunction(
        name=f"lf_synonyms_lowercase_tokens_negation_{descr_id}",
        f=check_synonyms_lowercase_tokens_negation_lf,
        resources=dict(descr_synon=descr_synon),
    )
