def __merge_predictions(prediction_list, prediction_list2):
    for prediction in prediction_list2:
        if not any(prediction["name"] in obj["name"] for obj in prediction_list):
            prediction_list.append(prediction)

    return prediction_list


def get_hotel_with_distance(object_concept, limit=25):

    if not object_concept:
        return None

    prediction = []

    for hotel in object_concept.intension.all():
        prediction.append({
            "name": hotel.name,
            "distance": 0
        })

    # Getting more specific hotels
    if len(prediction) < limit:
        prediction = __merge_predictions(prediction, get_hotels_down(object_concept))

    return prediction


def get_hotels_down(fc, distance=1):
    prediction = []
    sons = fc.son.all()

    for son in sons:
        for son_hotel in son.intension.all():
            # if son hotel name present in name atribute of an object in prediction list
            prediction = __add_if_not_present(son_hotel, prediction, distance)

    if len(prediction) < 25:
        for son in sons:
            prediction = __merge_predictions(prediction, get_hotels_down(son))

    return prediction


def __add_if_not_present(hotel, prediction, distance=0):
    if not any(hotel.name == obj["name"] for obj in prediction):
        prediction.append({
            "name": hotel.name,
            "distance": distance
        })

    return prediction


predictions = {
    "withDistance": get_hotel_with_distance
}