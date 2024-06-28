import numpy as np
def train_test_split_by_col(train_df, test_df, X_cols, y_col, model_name = None):

    X_train, X_test, y_train, y_test = train_df[X_cols], test_df[X_cols], train_df[y_col], test_df[y_col]
    if model_name == 'cat':
        return X_train, X_test, y_train, y_test.tolist()

    return X_train.values, X_test.values, np.array(y_train.tolist()), np.array(y_test.tolist())
