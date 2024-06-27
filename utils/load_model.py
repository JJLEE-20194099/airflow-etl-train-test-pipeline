from src.models.single_model import cat
from src.models.single_model import xgb
from src.models.single_model import lgbm
from src.models.single_model import abr
from src.models.single_model import etr
from src.models.single_model import gbr
from src.models.single_model import knr
from src.models.single_model import la
from src.models.single_model import mlp
from src.models.single_model import rf
from src.models.single_model import linear


def init(model_name, pretrained_file, categorical_features_indices):
    if model_name == 'abr':
        return abr.create_model(pretrained_file = pretrained_file)
    if model_name == 'cat':
        return cat.create_model(cat_idxs = categorical_features_indices, pretrained_file = pretrained_file)
    if model_name == 'etr':
        return etr.create_model(pretrained_file = pretrained_file)
    if model_name == 'gbr':
        return gbr.create_model(pretrained_file = pretrained_file)
    if model_name == 'knr':
        return knr.create_model(pretrained_file = pretrained_file)
    if model_name == 'la':
        return la.create_model(pretrained_file = pretrained_file)
    if model_name == 'lgbm':
        return lgbm.create_model(pretrained_file = pretrained_file)
    if model_name == 'linear':
        return linear.create_model(pretrained_file = pretrained_file)
    if model_name == 'mlp':
        return mlp.create_model(pretrained_file = pretrained_file)
    if model_name == 'rf':
        return rf.create_model(pretrained_file = pretrained_file)
    if model_name == 'xgb':
        return xgb.create_model(pretrained_file = pretrained_file)