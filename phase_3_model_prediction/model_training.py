#!/usr/bin/env python
# coding: utf-8

# In[9]:


import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import matplotlib.pyplot as plt

# Load the feature-engineered data without parsing dates
df_tra_Y_tr_time = pd.read_csv('feature_engineered_data.csv', index_col=0)

# Define features and target
features = ['hour', 'day_of_week', 'quarter', 'month', 'year', 'rolling_mean_3', 'rolling_mean_6', 'rolling_mean_12']
target = 'Location_0'

# Split the data into training and testing sets
X = df_tra_Y_tr_time[features]
y = df_tra_Y_tr_time[target]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train the model
model = LinearRegression()
model.fit(X_train, y_train)

# Save the trained model
joblib.dump(model, 'traffic_flow_model.pkl')


# In[ ]:





# In[ ]:




