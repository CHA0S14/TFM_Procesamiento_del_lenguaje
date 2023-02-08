from utils import utils, score
import os
from experiment.Experiment import Experiment
import json

data_df, metadata_df, ground_df = utils.load_data("dataset/test.csv",
                                                  "dataset/item_metadata.csv",
                                                  "dataset/confirmation.csv")

results_folder = "./Results"

if not os.path.exists(results_folder):
    os.mkdir(results_folder)

full_experiment_result = f"{results_folder}/FullExperiment"
num_users = 1000

experiment_users = []

if os.path.exists(f'{full_experiment_result}/experiment_users.txt'):
    with open(f'{full_experiment_result}/experiment_users.txt', "r") as f:
        experiment_users = [line.strip('\n') for line in f]
else:
    experiment_users = utils.extract_random_users(data_df, ground_df['user_id'].unique(), num_users, 12091994)

    # If folder dont exist, create it
    if not os.path.exists(full_experiment_result):
        os.mkdir(full_experiment_result)

    # Save experiment_users to file
    with open(f'{full_experiment_result}/experiment_users.txt', "w") as f:
        for user in experiment_users:
            f.write(str(user) + "\n")

print(f'Users selected: {experiment_users}')

experiments = {
    "basic": {
        "ponderation": "noPonderation",
        "score": None
    },
    "distancePonderated": {
        "ponderation": "distancePonderated",
        "score": None
    },
    "visitedPonderated": {
        "ponderation": "clickedPonderated",
        "score": None
    },
    "mixed": {
        "ponderation": "mixedPonderated",
        "score": None
    }
}

for name, configuration in experiments.items():
    print(f"Running experiment {name} with ponderation {configuration['ponderation']}")

    experiment_folder = f"{full_experiment_result}/{name}"

    experiment = Experiment(folder=experiment_folder, data_df=data_df,
                            experiment_users=experiment_users, ponderation=configuration["ponderation"], jobs=10)

    experiment.init_experiment()

    total_time, mean_time = utils.calculate_time(experiment_folder)

    configuration["Total time"] = "{:.4f}s".format(total_time)
    configuration["Mean time"] = "{:.4f}s".format(mean_time)

    configuration["score"] = score.score(f"{experiment_folder}/submission.csv", f"dataset/confirmation.csv")

    with open(f'{full_experiment_result}/{name}_results.json', "w") as f:
        f.write(json.dumps(configuration, indent=4))
