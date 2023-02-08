import os
import csv
import pandas as pd
import numpy as np
import threading

locks = {
    'submission': threading.Lock(),
    'log': threading.Lock(),
    'times': threading.Lock()
}


def append_csv(header, line, path, filename):
    os.makedirs(path, exist_ok=True)

    filename = f'{path}/{filename}'

    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            write = csv.writer(f, lineterminator='\n')
            write.writerow(header)

    with open(filename, 'a') as f:
        write = csv.writer(f, lineterminator='\n')
        write.writerow(line)


def load_data(data_path, metadata_path, ground_data_path):
    print("Loading data...")
    data_df = pd.read_csv(data_path)
    metadata_df = pd.read_csv(metadata_path)
    ground_df = pd.read_csv(ground_data_path)

    return data_df, metadata_df, ground_df


def extract_random_users(data_df, users_available, num_users, seed):
    print("Extracting users...")
    np.random.seed(seed)

    list_to_extract_from = data_df[(data_df['action_type'] == 'clickout item')
                                   & (data_df['reference'].isnull())
                                   & (data_df['user_id'].isin(users_available))]

    return np.random.choice(list_to_extract_from['user_id'].unique(), num_users, replace=False)


def get_user_info(user, data_df):
    # Preparing even_type list containing:
    #   - interaction item image
    # 	- interaction item info
    # 	- interaction item deals
    # 	- interaction item rating
    # 	- search for item
    # 	- clickout item

    last_user_session = data_df[(data_df['user_id'] == user) & (data_df['action_type'] == 'clickout item') & (
        data_df['reference'].isnull())].tail(1)

    session_id = last_user_session["session_id"].values[0]
    timestamp = last_user_session["timestamp"].values[0]
    step = last_user_session["step"].values[0]

    return session_id, timestamp, step, "Done!"


def extract_impressions(data_df, user):
    print(f'{user}: Extracting impressions...')
    # Getting action type is clickout item and reference is null
    data_to_guess = data_df[(data_df['user_id'] == user) & (data_df['action_type'] == 'clickout item') & (
        data_df['reference'].isnull())].tail(1)

    # Getting impressions and split the string by | into a list
    return data_to_guess['impressions'].values[0].split('|')


def save_submission(user_id, session_id, timestamp, step, item_recommendations, result_path='../submissions'):
    locks["submission"].acquire()

    append_csv(
        ['user_id', 'session_id', 'timestamp', 'step', 'item_recommendations'],
        [user_id, session_id, timestamp, step, item_recommendations],
        result_path,
        "submission.csv")

    locks["submission"].release()


def to_log(message, path='./logs', log_name='log.txt'):
    locks["log"].acquire()

    if not os.path.exists(path):
        os.makedirs(path)

    with open(f'{path}/{log_name}', 'a') as f:
        f.write(f'{message}\r\n')

    locks["log"].release()


def is_in_csv(folder, user):
    locks["submission"].acquire()

    path = f'{folder}/submission.csv'
    if not os.path.exists(path):
        locks["submission"].release()
        return False

    df = pd.read_csv(path)

    if user in df['user_id'].values:
        locks["submission"].release()
        print(f'{user}: Already in csv! (Skipping...')
        return True

    locks["submission"].release()
    return False


def write_time(user, time, path='./times', filename='times.csv'):
    locks["times"].acquire()

    append_csv(
        ['user_id', 'time'],
        [user, time],
        path,
        filename)

    locks["times"].release()


def calculate_time(path='./times', filename='times.csv'):
    df = pd.read_csv(f'{path}/{filename}')

    return df['time'].sum(), df['time'].mean()
