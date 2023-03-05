def clicked_penalization(user, hotel, ranking, data_df, action_types=None):
    count_clicked = __get_clicked_count(user, hotel, ranking, data_df, action_types)

    # If user didn't click the hotel, return ranking
    if count_clicked == 0:
        return ranking

    return ranking / count_clicked


def clicked_bonus(user, hotel, ranking, data_df, action_types=None):
    return ranking + __get_clicked_count(user, hotel, ranking, data_df, action_types)


def __get_clicked_count(user, hotel, ranking, data_df, action_types=None):
    # Count reference is hotel and action_type is in action_types
    if action_types is None:
        action_types = ['clickout item']

    return data_df[(data_df['user_id'] == user) & (data_df['reference'] == hotel["name"]) & (
        data_df['action_type'].isin(action_types))].shape[0]


def ponderated_by_distance(user, hotel, ranking, data_df, action_types=None, penalization=0.5):
    # if hotel distance is 0, return ranking
    if hotel["distance"] == 0:
        return ranking

    return ranking * (penalization / hotel["distance"])


def mixed_penalization_ponderation(user, hotel, ranking, data_df, action_types=None, penalization=0.5):

    by_clicked = clicked_penalization(user, hotel, ranking, data_df, action_types)

    return ponderated_by_distance(user, hotel, by_clicked, data_df, action_types, penalization)


def mixed_bonus_ponderation(user, hotel, ranking, data_df, action_types=None, penalization=0.5):

    by_clicked = clicked_bonus(user, hotel, ranking, data_df, action_types)

    return ponderated_by_distance(user, hotel, by_clicked, data_df, action_types, penalization)


def no_ponderation(user, hotel, ranking, data_df):
    return ranking


ponderations = {
        "clickedPenalization": clicked_penalization,
        "distancePonderated": ponderated_by_distance,
        "mixedPenalizationPonderated": mixed_penalization_ponderation,
        "noPonderation": no_ponderation,
        "clickedBonus": clicked_bonus,
        "mixedBonusPonderated": mixed_bonus_ponderation,
    }