+++
author = "Plamen Rabadzhiyski"
date = 2023-01-05T23:00:00Z
draft = true
image = "/uploads/laptop_chart.jpg"
include_cta = false
title = "Predicting customer churn with Spark"

+++
Let’s use Spark’s capabilities to predict customer churn of a company that provides online music services. The company has two main types of customers — Free and Paid users. Any user can upgrade or downgrade the service at any time. The company stores a decent amount of data that can be used to design a machine-learning model to predict what customers would churn so that we can offer them incentives and make them stay as long as possible.

The project uses PySpark libraries and it was developed with Jupyter notebooks on a local PC. A Spark cluster was also used in [IBM Watson Studio](https://www.ibm.com/cloud/watson-studio). There are three data sets available with customer data — mini, medium, and a 12GB data set, provided by [Udacity](https://www.udacity.com/). The mini version was used locally, and the medium-sized data was used in the cluster. The project consists of:

* Jupyter Notebook — where all code is available (working and final version)
* Blog post — this post
* [Github](https://github.com/rabadzhiyski/SparkProject) repository — where all code can be found

Customer churn is addressed through data transformation, feature engineering, and machine learning classification. The best model was chosen based on the AUC-ROC metric. Logistic regression and Random Forest classifier were tested. In the end, the Logistic regression model performed 23% better than the Random Forest classifier.