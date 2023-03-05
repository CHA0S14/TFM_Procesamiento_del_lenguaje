from experiment.Experiment import Experiment
from entities import data_model as data_model
from utils import utils
from joblib import Parallel, delayed
import time

class FrecuencyBaseLine(Experiment):

    def __get_number_user_clicked(self, impression):
        filter = (self.data_df['reference'] == impression) & (self.data_df['action_type'] == 'clickout item')
        return len(self.data_df[filter])

    def __ordered_by_clicked(self, impressions):
        ranking = []

        for impression in impressions:
            ranking.append({
                'impression': impression,
                'clicks': self.__get_number_user_clicked(impression)
            })

        ranking.sort(key=lambda x: x['clicks'], reverse=True)

        return [x['impression'] for x in ranking]

    def __make_prediction(self, index, user):
        uid = f'test{index}'

        if utils.is_in_csv(self.folder, user):
            return

        start = time.time()

        impressions = super().extract_impressions(user)

        if not impressions:
            return user, "Error obtaining impressions: empty list or NaN"
        
        prediction = self.__ordered_by_clicked(impressions)

        session_id, timestamp, step, result = utils.get_user_info(user, self.data_df)

        if session_id is None:
            return user, result

        if not prediction or len(prediction) == 0:
            print(f'{uid}: Could not make a prediction...')
            result = f"Empty prediction"

        # If result is not done, print error
        if result != "Done!":
            print(f'{user}: {result}')
            super().write_into_ko(user, result)
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
            delayed(self.__make_prediction)(index, user) for index, user in
            enumerate(self.experiment_users))

        print("Done!")

    def __init__(self, folder, data_df, experiment_users,
                 jobs=10, verbose=False,
                 prediction_length=25,
                 processed_data_path="./ProcessedData",
                 log_path="./log"):
                
        super().__init__(folder=folder, 
                         data_df=data_df, 
                         experiment_users=experiment_users, 
                         jobs=jobs, 
                         verbose=verbose, 
                         prediction_length=prediction_length, 
                         processed_data_path=processed_data_path, 
                         log_path=log_path)
