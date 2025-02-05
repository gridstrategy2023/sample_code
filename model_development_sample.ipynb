import pandas as pd
import numpy as np
import pickle
import shap

from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import mean_squared_error
import warnings

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder

from sklearn.model_selection import GridSearchCV
from sklearn.inspection import permutation_importance
from matplotlib import pyplot as plt
import seaborn as sns # for correlation heatmap

from sklearn.tree import DecisionTreeRegressor
from sklearn import tree
from sklearn.tree import _tree
from sklearn.tree import plot_tree

pd.set_option('display.max_rows', 500)

df=pd.read_pickle('/home/df_pre_issue.pkl')

# Create unique policy dataframe to split the policy into training and validatoin policy sets.
policy_df=df['policy']
policy_df.drop_duplicates(inplace=True)

pol_train_calib, pol_valid= train_test_split(policy_df, train_size=0.75, test_size=0.25, random_state=123)

pol_train, pol_calib=train_test_split(pol_train_calib, train_size=0.80, test_size=0.20, random_state=123)

# Merge data to randomly split policy to create training and validation data.
df_train=pd.merge(df,pol_train, how='inner', on='policy')
df_valid=pd.merge(df,pol_valid, how='inner', on='policy')
df_calib=pd.merge(df,pol_calib, how='inner', on='policy')

# Create train/validation data for regression model. No need for calibration data.
df_train_reg=pd.merge(df, pol_train_calib, how='inner', on='policy')
df_valid_reg=pd.merge(df, pol_valid, how='inner', on='policy')


#check columns with missing values

X=df[list_x]
cols_with_missing = [col for col in X.columns if X[col].isnull().any()]

print("Columns with missing values :", cols_with_missing)
print("X shape :", X.shape)

# Preprocessing numerical data
numerical_transformer = SimpleImputer(strategy='constant')

# Preprocessing categorical data
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

# Bundle preprocessing for numerical and categorical data
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numerical_cols),
        ('cat', categorical_transformer, categorical_cols)
    ])

# Grid search for best parameter on downsamped data
# Create parameter values for gridsearch - carefull, "model__" prepended defined in pipeline
model_xgb=xgb.XGBClassifier(random_state = 0) # default parameter setting
model_pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                              ('model', model_xgb)
                             ])


params = { 
    'model__max_depth': [2,3, 4],
    'model__learning_rate': [0.05, 0.1, 0.2],
    "model__gamma":[0, 0.25, 0.5, 0.75,1],
    'model__n_estimators': [100, 500, 1000],
}

# Use GridSearchCV for all combinations
grid = GridSearchCV(
    estimator = model_pipeline,
    param_grid = params,
    scoring = 'roc_auc',
    n_jobs = -1,
    cv = 4,
    verbose = 3,
)

# Best parameter search using downsampled data
grid.fit(X_train_clf_ds, y_train_clf_ds)

grid.best_params_

# Compute auc, recall
from sklearn.metrics import roc_curve,auc,recall_score,precision_score, roc_auc_score
from sklearn import metrics
from sklearn.metrics import average_precision_score, precision_recall_curve
from sklearn.metrics import auc, plot_precision_recall_curve
import matplotlib.pyplot as plt

def roc_auc(predict,label):
  roc_auc=roc_auc_score(label, predict)
  return roc_auc
  
def pr_auc(predict,label):
  precision, recall, thresholds = precision_recall_curve(label, predict)
  auc_precision_recall = auc(recall, precision)
  return auc_precision_recall

print('validation roc/auc: ', roc_auc(y_predict_test_proba_ds[:,1],y_valid_clf_ds))
print('validation pr/auc: ', pr_auc(y_predict_test_proba_ds[:,1], y_valid_clf_ds))

# Get feature importance from model pipeline: get importance and column names
imp_df=pd.DataFrame()
imp_df['importance']=model_pipeline_ds.steps[1][1].feature_importances_.tolist()
imp_df['colname']=col_names # column names after one-hot-encode
imp_df=imp_df.sort_values(by='importance', ascending=False)
imp_df.head(10)
# Create sample data for shap value
X_valid_shap = X_valid_clf_ds.sample(
    frac=0.1, 
    replace=True, 
    random_state=0
)

# Compute SHAP value
explainer = shap.Explainer(
    model_pipeline_ds["model"], 
    feature_names=model_pipeline_ds['preprocessor'].get_feature_names_out()
)

data_transformation = model_pipeline_ds['preprocessor'].transform(X_valid_shap)

shap_values = explainer(data_transformation)

shap.plots.waterfall(shap_values[0], max_display=10)