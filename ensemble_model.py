import mlflow

from src.helpers.mlflow_tool import create_experiment, get_best_model

experiment_name = "lgbm_realestate_test_training_phrase_1"
model, uri = get_best_model(experiment_name)

schema = mlflow.pyfunc.load_model(uri).metadata.signature
print(schema)
feature_names = Schema.input_names(schema.inputs)
target_name = Schema.input_names(schema.outputs)

print(feature_names, target_name)