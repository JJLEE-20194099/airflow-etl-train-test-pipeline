import mlflow
from tqdm import tqdm

experiments = mlflow.search_experiments(filter_string="name LIKE '%num_version_training'")

for exp in tqdm(experiments):
    mlflow.delete_experiment(experiment_id=exp.experiment_id)
