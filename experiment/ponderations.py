def ponderated_by_clicked(user, hotel, ranking, data_df, action_types=None):
    # Count reference is hotel and action_type is in action_types
    if action_types is None:
        action_types = ['clickout item']

    count_clicked = data_df[(data_df['user_id'] == user) & (data_df['reference'] == hotel["name"]) & (
        data_df['action_type'].isin(action_types))].shape[0]

    # If user didn't click the hotel, return ranking
    if count_clicked == 0:
        return ranking

    return ranking / count_clicked


def ponderated_by_distance(user, hotel, ranking, data_df, action_types=None, penalization=0.5):
    # if hotel distance is 0, return ranking
    if hotel["distance"] == 0:
        return ranking

    return ranking * (penalization / hotel["distance"])


def mixed_ponderation(user, hotel, ranking, data_df, action_types=None, penalization=0.5):

    by_clicked = ponderated_by_clicked(user, hotel, ranking, data_df, action_types)

    return ponderated_by_distance(user, hotel, by_clicked, data_df, action_types, penalization)


def no_ponderation(user, hotel, ranking, data_df):
    return ranking


ponderations = {
        "clickedPonderated": ponderated_by_clicked,
        "distancePonderated": ponderated_by_distance,
        "mixedPonderated": mixed_ponderation,
        "noPonderation": no_ponderation
    }