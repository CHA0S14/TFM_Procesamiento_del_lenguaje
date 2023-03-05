from entities import data_model as data_model
from utils import utils
import os
import numpy as np
from joblib import Parallel, delayed
from neomodel import db
from experiment.ponderations import ponderations
from experiment.predictions import predictions
import csv
from subprocess import Popen, PIPE
import shutil
import time


class Experiment:
    action_types = ['interaction item image', 'interaction item info', 'interaction item deals',
                    'interaction item rating', 'search for item', 'clickout item']

    def extract_users_clicked_impression(self, user, impressions):

        if self.verbose:
            print(f'{user}: Extracting users that clicked on impressions...')

        clicked_df = self.data_df[
            (self.data_df['reference'].isin(impressions)) & (self.data_df['action_type'].isin(self.action_types))]
        users = clicked_df.user_id.unique()

        # if user not in list add it
        if user not in users:
            users = np.append(users, user)

        return users.tolist()

    def get_clicked_history_from_user(self, user, user_list):
        if self.verbose:
            print(f'{user}: Extracting users click history...')

        # Get all the users from the list, action type is clicout item and reference is not null
        clicked_df = self.data_df[
            (self.data_df['user_id'].isin(user_list)) & (self.data_df['action_type'] == 'clickout item') & (
                self.data_df['reference'].notnull())]
        return clicked_df.reference.unique().tolist()

    def save_data_csv(self, uid, user, users_clicked_impression, hotel_history, impressions,
                        save_path=None):

        if not save_path:
            save_path = f"{self.processed_data_path}/metadata"

        if self.verbose:
            print(f'{user}: Saving data to csv...')

        data_uid = f'automatic_test_{uid}'

        path = f'{save_path}/{data_uid}'

        os.makedirs(path, exist_ok=True)

        with open(f'{path}/{data_uid}-relations.csv', 'w') as f:
            write = csv.writer(f, lineterminator='\n')
            for user in users_clicked_impression:
                clicked_elements = \
                    self.data_df[(self.data_df['user_id'] == user) & (self.data_df['reference'].notnull()) & (
                        self.data_df['action_type'].isin(self.action_types))]['reference'].unique()

                # Array with ids that will be 1
                user_relation_ids = Parallel(n_jobs=10)(
                    delayed(lambda elem: hotel_history.index(elem) if elem in hotel_history else None)(clicked_element)
                    for
                    clicked_element in clicked_elements)

                user_relation_ids = set(filter(None, user_relation_ids))

                write.writerow(user_relation_ids)

        with open(f'{path}/{data_uid}-objects.csv', 'w') as f:
            write = csv.writer(f, lineterminator='\n')
            write.writerow(users_clicked_impression)

        with open(f'{path}/{data_uid}-attributes.csv', 'w') as f:
            write = csv.writer(f, lineterminator='\n')
            write.writerow(hotel_history)

        with open(f'{path}/impresions.csv', 'w') as f:
            write = csv.writer(f, lineterminator='\n')
            write.writerow(impressions)

    def create_lattice(self, uid, db_name):
        if self.verbose:
            print(f'{uid}: Creating lattice and storing on database...')

        args = [self.jar_path, f'--spring.data.neo4j.database={db_name}', f'--uid={uid}',
                f'--dataPath={self.processed_data_path}/metadata']

        process = Popen(['java', '-Xmx4G', '-Xms1G', '-jar'] + list(args), stdout=PIPE, stderr=PIPE)

        stdout, stderr = process.communicate()

        if self.verbose:
            print(stdout.decode("utf-8"))
            print(stderr.decode("utf-8"))

        utils.to_log(f'{uid}: {stdout.decode("utf-8")}', self.log_path, 'java-latice.log')

    def process_user(self, uid, user, impressions):
        # Preparing even_type list containing:
        #   - interaction item image
        # 	- interaction item info
        # 	- interaction item deals
        # 	- interaction item rating
        # 	- search for item
        # 	- clickout item

        users_clicked_impression = self.extract_users_clicked_impression(user, impressions)

        if not users_clicked_impression:
            return None, None, None, "Error obtaining users who clicked impressions: empty list or NaN"

        hotel_history = self.get_clicked_history_from_user(user, users_clicked_impression)

        if not hotel_history:
            hotel_history = []

        self.save_data_csv(uid, user, users_clicked_impression, hotel_history, impressions)

        data_model.create_database(uid, db)

        self.create_lattice(f'automatic_test_{uid}', uid)

        return utils.get_user_info(user, self.data_df)

    def ranking_prediction(self, user, prediction, impressions):
        ranking = {}
        for hotel in prediction:
            attribute_concept, _ = data_model.get_attribute_concept(hotel["name"], db)

            if attribute_concept:
                ranking[hotel["name"]] = ponderations[self.ponderation](user, hotel,
                                                                        len(attribute_concept.extension.all()),
                                                                        self.data_df)
            else:
                ranking[hotel["name"]] = 0

        if len(ranking.items()) > 1:
            ranking = list({k: v for k, v in sorted(ranking.items(), key=lambda item: item[1], reverse=True)}.keys())

        ranking = [hotel for hotel in ranking if hotel in impressions]

        if len(ranking) > self.prediction_length:
            ranking = ranking[0:self.prediction_length]

        return ranking

    def extract_impressions(self, user):
        if self.verbose:
            print(f'{user}: Extracting impressions...')

        # Getting action type is clickout item and reference is null
        data_to_guess = self.data_df[
            (self.data_df['user_id'] == user) & (self.data_df['action_type'] == 'clickout item') & (
                self.data_df['reference'].isnull())].tail(1)

        # Getting impressions and split the string by | into a list
        return data_to_guess['impressions'].values[0].split('|')

    def clean(self, uid, metadata_path=None):
        if not metadata_path:
            metadata_path = f"{self.processed_data_path}/metadata"

        db.set_connection(f'{self.neo4j_uri}/neo4j')

        data_model.drop_database(uid, db)

        if self.verbose:
            print(f'{uid}: Cleaning metadata folder...')

        data_uid = f'automatic_test_{uid}'

        uid_path = f'{metadata_path}/{data_uid}'

        shutil.rmtree(uid_path)

    def write_into_ko(self, user, message):
        utils.append_csv(['user_id', 'message'], [user, message], f"{self.folder}/ko", "error.csv")

    def make_prediction(self, index, user):
        uid = f'test{index}'

        if utils.is_in_csv(self.folder, user):
            return

        start = time.time()

        impressions = self.extract_impressions(user)

        if not impressions:
            return user, "Error obtaining impressions: empty list or NaN"

        session_id, timestamp, step, result = self.process_user(uid, user, impressions)

        if session_id is None:
            return user, result

        db.set_connection(f'{self.neo4j_uri}/{uid}')

        object_concept, user_fcs = data_model.get_object_concept(user, db)

        prediction = predictions[self.prediction](object_concept)

        if not prediction or len(prediction) == 0 or not any(
                item in [obj["name"] for obj in prediction] for item in impressions):
            prediction = [{"name": impression, "distance": 0} for impression in impressions]

        prediction = self.ranking_prediction(user, prediction, impressions)

        if not prediction or len(prediction) == 0:
            print(f'{uid}: Could not make a prediction...')
            result = f"Empty prediction"

        self.clean(uid)

        # If result is not done, print error
        if result != "Done!":
            print(f'{user}: {result}')
            self.write_into_ko(user, result)
            return

        submission = [user, session_id, timestamp, step, ' '.join(prediction)]

        if submission and len(submission) == 5:
            user, session_id, timestamp, step, impressions = submission
            utils.save_submission(user, session_id, timestamp, step, impressions, self.folder)
        else:
            print(f'Error: {submission}')

        end = time.time()

        utils.write_time(user, end - start, self.folder)

    def init_experiment(self):
        data_model.connect(self.neo4j_uri)

        # Parallel process users
        print("Processing users...")
        Parallel(n_jobs=self.jobs, verbose=10, backend="threading")(
            delayed(self.make_prediction)(index, user) for index, user in
            enumerate(self.experiment_users))

        print("Done!")

    def __init__(self, folder, data_df, experiment_users, ponderation="noPonderation", prediction="withDistance",
                 jobs=10,
                 neo4j_uri="bolt://neo4j:neo4j2@localhost:7687", verbose=False,
                 jar_path="lib/TFM-2022-0.0.1-SNAPSHOT.jar", prediction_length=25,
                 processed_data_path="./ProcessedData",
                 log_path="./log"):
        self.data_df = data_df
        self.experiment_users = experiment_users
        self.folder = folder
        self.ponderation = ponderation
        self.prediction = prediction
        self.jobs = jobs
        self.neo4j_uri = neo4j_uri
        self.verbose = verbose
        self.jar_path = jar_path
        self.prediction_length = prediction_length
        self.processed_data_path = processed_data_path
        self.log_path = log_path

        # If folder dont exist, create it
        if not os.path.exists("../Result"):
            os.mkdir("../Result")
