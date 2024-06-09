from src.helpers.agg_tool import mean_max, stack_with_onehot
from src.helpers.mlflow_tool import load_model_or_uri
import mlflow.pyfunc
import copy

class Ensemble(mlflow.pyfunc.PythonModel):
    def __init__(self, models_list, ensemble_method=mean_max, stack_scores=stack_with_onehot,
                 models_all_cached=False, force_predict_function=False):
        super().__init__()
        if models_all_cached:
            self.models_list = []
            for model in models_list:
                self.models_list.append(load_model_or_uri(model))
        else:
            self.models_list = models_list

        self.scores_list = None
        self.meta_model = None
        self._ensemble_method = ensemble_method
        self._stack_scores = stack_scores
        self._force_predict_function = force_predict_function

    def predict_scores(self, data, cache_scores=False):
        scores_list = []
        for model in self.models_list:
            loaded_model = load_model_or_uri(model)
            if not self._force_predict_function:
                if hasattr(loaded_model, "_model_impl") and hasattr(loaded_model._model_impl, "predict_proba"):
                    scores = loaded_model._model_impl.predict_proba(data)
                elif hasattr(loaded_model, "predict_proba"):
                    scores = loaded_model.predict_proba(data)
                else:
                    scores = loaded_model.predict(data)
            else:
                scores = loaded_model.predict(data)

            scores_list.append(scores)

        if cache_scores:
            self.scores_list = scores_list
        return scores_list

    def aggregate_scores(self, scores):
        if self._ensemble_method == "meta_model":
            return self.meta_model.predict(scores)
        else:
            return self._ensemble_method(scores)

    def fit(self, model, data, target, force_scores_compute=True, cache_scores=True):
        if force_scores_compute or self.scores_list is None:
            data = self.predict_scores(data, cache_scores)
        else:
            data = self.scores_list

        data = self._stack_scores(data)
        model.fit(data, target)
        self.meta_model = model

    def set_meta_model(self, model):
        self.meta_model = copy.deepcopy(model)

    def predict(self, context, data):
        scores = self.predict_scores(data, cache_scores=False)
        scores = self._stack_scores(scores)
        return self.aggregate_scores(scores)