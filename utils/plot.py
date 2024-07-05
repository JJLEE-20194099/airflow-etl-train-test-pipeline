import matplotlib.pylab as plt
import numpy as np
import pandas as pd
import seaborn as sns
import pathlib
from scipy import stats
from sklearn.linear_model import Ridge
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_squared_log_error,
    median_absolute_error,
    r2_score,
)

def plot_prediction_error(y_test, y_pred, style="seaborn", plot_size=(30, 24)):
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        ax.scatter(y_pred, y_test - y_pred)
        ax.axhline(y=0, color="red", linestyle="--")
        ax.set_title("Prediction Error Plot", fontsize=14)
        ax.set_xlabel("Predicted Values", fontsize=12)
        ax.set_ylabel("Errors", fontsize=12)
        plt.tight_layout()
    plt.close(fig)
    return fig

def plot_residuals(y_test, y_pred, style="seaborn", plot_size=(30, 24)):
    residuals = y_test - y_pred

    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        sns.residplot(
            x=y_pred,
            y=residuals,
            lowess=True,
            ax=ax,
            line_kws={"color": "red", "lw": 1},
        )

        ax.axhline(y=0, color="black", linestyle="--")
        ax.set_title("Residual Plot", fontsize=14)
        ax.set_xlabel("Predicted values", fontsize=12)
        ax.set_ylabel("Residuals", fontsize=12)

        for label in ax.get_xticklabels() + ax.get_yticklabels():
            label.set_fontsize(10)

        plt.tight_layout()

    plt.close(fig)
    return fig

def plot_coefficients(model, feature_names, style="seaborn", plot_size=(30, 24)):
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        ax.barh(feature_names, model.coef_)
        ax.set_title("Coefficient Plot", fontsize=14)
        ax.set_xlabel("Coefficient Value", fontsize=12)
        ax.set_ylabel("Features", fontsize=12)
        plt.tight_layout()
    plt.close(fig)
    return fig

def plot_correlation_matrix_and_save(
    df, style="seaborn", plot_size=(60, 60), path="/tmp/corr_plot.png"
):
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)

        # Calculate the correlation matrix
        corr = df.corr()

        # Generate a mask for the upper triangle
        mask = np.triu(np.ones_like(corr, dtype=bool))

        # Draw the heatmap with the mask and correct aspect ratio
        sns.heatmap(
            corr,
            mask=mask,
            cmap="coolwarm",
            vmax=0.3,
            center=0,
            square=True,
            linewidths=0.5,
            annot=True,
            fmt=".2f",
        )

        ax.set_title("Feature Correlation Matrix", fontsize=14)
        plt.tight_layout()

    plt.close(fig)
    # convert to filesystem path spec for os compatibility
    save_path = pathlib.Path(path)
    fig.savefig(save_path)


def plot_qq(y_test, y_pred, style="seaborn", plot_size=(30, 24)):
    residuals = y_test - y_pred
    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        stats.probplot(residuals, dist="norm", plot=ax)
        ax.set_title("QQ Plot", fontsize=14)
        plt.tight_layout()
    plt.close(fig)
    return fig

def plot_time_series(data, x_col, y_col, y_label_name, style="seaborn", plot_size=(16, 12)):
    if not isinstance(data, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    df = data.copy()

    try:
        df["date"] = pd.to_datetime(df[x_col])
    except:
        df["date"] = df[x_col]


    with plt.style.context(style=style):
        fig, ax = plt.subplots(figsize=plot_size)
        # Plot the original time series data with low alpha (transparency)
        ax.plot(df["date"], df[y_col], "b-o", label=f"{y_label_name}", alpha=0.15)
        # Plot the rolling average
        ax.plot(
            df["date"],
            df[y_col],
            "r",
            label=f"{y_label_name}",
        )

        # Set labels and title
        ax.set_title(
            f"Time Series Plot of {y_label_name}",
            fontsize=14,
        )
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel(f"{y_label_name}", fontsize=12)

        # Add legend to explain the lines
        ax.legend()
        plt.tight_layout()

    plt.close(fig)
    return fig