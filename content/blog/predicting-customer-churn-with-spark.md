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

## Problem statement

Customer churn is a very challenging area and it gives many opportunities for analyses. I used PySpark to load, transform data and build a machine-learning algorithm to predict users’ churn. It was important to find a way to get a realistic probability for users who are prompt to churn based on some features like gender, location, workday, songs played, etc.

With the help of several PySpark libraries, I explored the data, engineered the most appropriate features, designed a machine learning pipeline, and chose the most appropriate model for predicting churn. It is a classification task that required Logistic Regression, Random Forest Classifier, or another classification model.

## Metrics

To define if I worked correctly and if we can count on the ML model I needed some metrics. For this project, I used the area under AUC-ROC as a performance metric.

Even if accuracy is a very popular metric, it’s not the best choice for binary classification problems which often produce unbalanced data. Like fraud or spam filters, the customer churn data has classes that are distributed unequally.

For example, when we run our logistic model it had remarkable accuracy of 99% but AUC-ROC= 65%.

ROC (receiver operating curve) is the visual representation of the performance of the binary classifier. False Positive Rate vs True Positive Rate is plotted to get a visual understanding of the classifier’s performance. I chose Logistic regression over Random Forest as it was 23% more performant (0.65 vs 0.5).

## Data Exploration

We have three data sets — mini, medium, and big (12GB) data sets. For the sake of the exercise, we first started with a mini version of the data. The medium one was used with [IBM Watson Studio ](https://www.ibm.com/cloud/watson-studio)— a free cluster with Spark.

After the data is loaded we can have a look at the schema.

    df.printSchema()root
     |-- artist: string (nullable = true)
     |-- auth: string (nullable = true)
     |-- firstName: string (nullable = true)
     |-- gender: string (nullable = true)
     |-- itemInSession: long (nullable = true)
     |-- lastName: string (nullable = true)
     |-- length: double (nullable = true)
     |-- level: string (nullable = true)
     |-- location: string (nullable = true)
     |-- method: string (nullable = true)
     |-- page: string (nullable = true)
     |-- registration: long (nullable = true)
     |-- sessionId: long (nullable = true)
     |-- song: string (nullable = true)
     |-- status: long (nullable = true)
     |-- ts: long (nullable = true)
     |-- userAgent: string (nullable = true)
     |-- userId: string (nullable = true)

After the data is loaded we can have a look at the schema.

    # Check the newly created columns
    df.select('hour', 'day', 'workday_', 'month').show(5)+----+---+--------+-----+
    |hour|day|workday_|month|
    +----+---+--------+-----+
    |   0|  1|  Monday|   10|
    |   1|  1|  Monday|   10|
    |   1|  1|  Monday|   10|
    |   3|  1|  Monday|   10|
    |   4|  1|  Monday|   10|
    +----+---+--------+-----+
    only showing top 5 rows