import ast
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score, LeaveOneOut
import pandas as pd


def create_cat_dict(data):
    categories = []
    for i in range(data.shape[0]):
        cats = ast.literal_eval(data.iloc[i, 0])
        categories.extend(cats)
    categories = set(categories)
    categories_dict = {}
    for i, category in enumerate(categories):
        categories_dict[category] = i
    return categories_dict


def convert_categories_to_numerical(x_data, cat_dict):
    data_x_numerical = x_data.copy()
    for i in range(data_x_numerical.shape[0]):
        cats = ast.literal_eval(data_x_numerical.iloc[i]["Categories"])
        data_x_numerical.iloc[i, 0] = [cat_dict[c] for c in cats]
    return data_x_numerical


def create_list_array(df):
    array = np.array(df.iloc[:, 0])
    MLB = MultiLabelBinarizer()
    out = pd.DataFrame(MLB.fit_transform(array), columns=MLB.classes_)
    out.index = df.index
    return out


def predict_missing_values_tree(df, columns):
    data_x = df[["Categories"]]
    data_y = df[["Area"]]
    dic = create_cat_dict(data_x)
    x_data = convert_categories_to_numerical(data_x, dic)
    narray = create_list_array(x_data)
    array = np.array(narray.join(data_y))
    # Split the array into features (X) and target (y)
    X = array[:, :columns]
    y = array[:, columns]

    # Remove rows containing NaN values from X and y
    not_nan_indices = ~np.isnan(y)
    X = X[not_nan_indices, :]
    y = y[not_nan_indices]

    # Train the random forest regressor on the data
    model = RandomForestRegressor(n_estimators=1000).fit(X, y)
    loo = LeaveOneOut()
    n = loo.get_n_splits(y)
    scores = cross_val_score(model, X, y, cv=n, scoring="neg_mean_squared_error")
    # Get the indices of missing values in the final column
    missing_indices = np.where(np.isnan(array[:, columns]))[0]

    # Predict the missing values using the trained model
    predicted_values = model.predict(array[missing_indices, :columns])

    # Fill in the missing values in the final column
    array[missing_indices, columns] = predicted_values

    return array, scores.mean()


if __name__ == "__main__":
    df = pd.read_csv("DLIT_outputs/data.csv")
    filled, accuracy = predict_missing_values_tree(df, 31)
