import numpy as np
import pandas as pd
from pathlib import Path


def __read_into_df(file):
    """Read csv file into data frame."""
    df = (
        pd.read_csv(file).set_index(['user_id', 'session_id', 'timestamp', 'step'])
    )

    return df


def __generate_rranks_range(start, end):
    """Generate reciprocal ranks for a given list length."""

    return 1.0 / (np.arange(start, end) + 1)


def __convert_string_to_list(df, col, new_col):
    """Convert column from string to list format."""
    fxn = lambda arr_string: [int(item) for item in str(arr_string).split(" ")]

    mask = ~(df[col].isnull())

    df[new_col] = df[col]
    df.loc[mask, new_col] = df[mask][col].map(fxn)

    return df


def __get_reciprocal_ranks(ps):
    """Calculate reciprocal ranks for recommendations."""
    mask = ps.reference == np.array(ps.item_recommendations)

    if mask.sum() == 1:
        rranks = __generate_rranks_range(0, len(ps.item_recommendations))
        return np.array(rranks)[mask].min()
    else:
        return 0.0


def __get_average_precision_at_x(ps, x):
    """Calculate average precision at x."""
    ap3 = (ps.reference == np.array(ps.item_recommendations)[0:x]).sum() / x

    return ap3


def __score_submissions(subm_csv, gt_csv, verbose=False):
    """Score submissions with given objective function."""

    if verbose:
        print(f"Reading ground truth data {gt_csv} ...")

    df_gt = __read_into_df(gt_csv)

    if verbose:
        print(f"Reading submission data {subm_csv} ...")

    df_subm = __read_into_df(subm_csv)

    if verbose:
        print(f"create dataframe containing the ground truth to target rows")

    cols = ['reference', 'impressions', 'prices']
    df_key = df_gt.loc[:, cols]

    if verbose:
        print(f"append key to submission file")

    df_subm_with_key = df_key.join(df_subm, how='inner')
    df_subm_with_key.reference = df_subm_with_key.reference.astype(int)
    df_subm_with_key = __convert_string_to_list(df_subm_with_key, 'item_recommendations', 'item_recommendations')

    result = []

    # score each row
    if verbose:
        print(f"Reciprocal scoring ...")

    df_subm_with_key['rr'] = df_subm_with_key.apply(__get_reciprocal_ranks, axis=1)

    result.append(round(df_subm_with_key.rr.mean(), 4))

    df_subm_with_key["ap1"] = df_subm_with_key.apply(__get_average_precision_at_x, axis=1, x=1)
    result.append(round(df_subm_with_key["ap1"].mean(), 4))

    for i in range(5, 21, 5):
        if verbose:
            print(f"Average precision at {i} scoring ...")
        key = f'ap{i}'
        df_subm_with_key[key] = df_subm_with_key.apply(__get_average_precision_at_x, axis=1, x=i)

        result.append(round(df_subm_with_key[key].mean(), 4))

    return result


def score(submission_file, ground_truth_file):

    # calculate path to files
    gt_csv = Path(ground_truth_file)
    subm_csv = Path(submission_file)

    mrr, map1, map5, map10, map15, map20 = __score_submissions(subm_csv, gt_csv)

    return {
        'mrr': mrr,
        'map@1': map1,
        'map@5': map5,
        'map@10': map10,
        'map@15': map15,
        'map@20': map20
    }
