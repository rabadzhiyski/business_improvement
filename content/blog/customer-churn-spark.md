---
author: Plamen Rabadzhiyski
date: "2021-06-04T08:45:27+06:00"
image: images/illustrations/laptop_chart.jpg
include_cta: true
title: Predicting Customer Churn with PySpark
draft: true 
editor_options: 
  markdown: 
    wrap: 72
---

## Technical report with code chunks

I used
[PySpark](https://spark.apache.org/docs/latest/api/python/index.html) to
predict customer churn of a company that provides online music services.
The company has two main types of customers --- Free and Paid users. Any
user can upgrade or downgrade the service at any time. The company
stores a decent amount of data that can be used to design a machine
learning model to predict what customers would churn so that we can
offer them incentives and make them stay as long as possible.

The project uses [PySpark](https://www.databricks.com/glossary/pyspark)
libraries and it was developed with Jupyter notebooks on a local PC. A
Spark cluster was also used in [IBM Watson
Studio](https://www.ibm.com/cloud/watson-studio). There are three data
sets available with customer data --- mini, medium, and a 12GB data set,
provided by [Udacity](https://www.udacity.com/). The mini version was
used locally, and the medium-sized data was used in the cluster. The
project consists of:

-   Jupyter Notebook --- where all code is available (working and final
    version)

-   Blog post --- this post

-   [Github](https://github.com/rabadzhiyski/SparkProject) repository
    --- where all code can be found

The customer churn is addressed through data transformation, feature
engineering, and machine learning classification. The best model was
chosen based on the AUC-ROC metric. Logistic regression and Random
Forest classifier were tested. In the end, the Logistic regression model
performed 23% better than the Random Forest classifier.

------------------------------------------------------------------------

## Problem statement

Customer churn is a very challenging area and it gives many
opportunities for analyses. I used PySpark to load, transform data and
build a machine-learning algorithm to predict users' churn. It was
important to find a way to get a realistic probability for users who are
prompt to churn based on some features like gender, location, workday,
songs played, etc.

With the help of several PySpark libraries, I explored the data,
engineered the most appropriate features, designed a machine learning
pipeline, and chose the most appropriate model for predicting churn. It
is a classification task that required Logistic Regression, Random
Forest Classifier, or another classification model.

## Metrics

To define if I worked correctly and if we can count on the ML model I
needed some metrics. For this project, I used the area under AUC-ROC as
a performance metric.

Even if accuracy is a very popular metric, it's not the best choice for
binary classification problems which often produce unbalanced data. Like
fraud or spam filters, the customer churn data has classes that are
distributed unequally.

For example, when we run our logistic model it had remarkable accuracy
of 99% but AUC-ROC= 65%.

ROC (receiver operating curve) is the visual representation of the
performance of the binary classifier. False Positive Rate vs True
Positive Rate is plotted to get a visual understanding of the
classifier's performance. I chose Logistic regression over Random Forest
as it was 23% more performant (0.65 vs 0.5).

## Data Exploration

We have three data sets --- mini, medium, and big (12GB) data set. For
the sake of the exercise, we first started with a mini version of the
data. The medium one was used with [IBM Watson
Studio](https://www.ibm.com/cloud/watson-studio)--- a free cluster with
Spark.

After data is loaded we can have a look at the schema.

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
     


One useful step before jumping to any exploration is to make sure that
the "ts" column is converted to a human-readable date format. We used
some user-defined functions to do that.

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

Now we're good to go.

It would be interesting and useful to dig into the data a little bit.

***How many songs do users listen to on average between visiting the
home page?***

    function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

    user_window = Window \
        .partitionBy('userID') \
        .orderBy(desc('ts')) \
        .rangeBetween(Window.unboundedPreceding, 0)

    cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
        .select('userID', 'page', 'ts') \
        .withColumn('homevisit', function(col('page'))) \
        .withColumn('period', Fsum('homevisit').over(user_window))

    cusum.filter((cusum.page == 'NextSong')) \
        .groupBy('userID', 'period') \
        .agg({'period':'count'}) \
        .agg({'count(period)':'avg'}).show()

    +------------------+
    |avg(count(period))|
    +------------------+
    | 23.66741388737015|
    +------------------+

***What are the top 5 played artists?***

    # top 5 played artist
    df.filter(df.page == 'NextSong') \
        .select('Artist') \
        .groupBy('Artist') \
        .agg({'Artist':'count'}) \
        .withColumnRenamed('count(Artist)', 'Artistcount') \
        .sort(desc('Artistcount')) \
        .show(5)

    +--------------------+-----------+
    |              Artist|Artistcount|
    +--------------------+-----------+
    |       Kings Of Leon|       3497|
    |            Coldplay|       3439|
    |Florence + The Ma...|       2314|
    |                Muse|       2194|
    |       Dwight Yoakam|       2187|
    +--------------------+-----------+
    only showing top 5 rows

We can use previously created date columns to see some patterns in
customer behaviors. But before doing that we need to create a customer
churn column.

    # check the page column df.select("page").dropDuplicates().sort("page").show()+--------------------+
    |                page|
    +--------------------+
    |               About|
    |          Add Friend|
    |     Add to Playlist|
    |              Cancel|
    |Cancellation Conf...|
    |           Downgrade|
    |               Error|
    |                Help|
    |                Home|
    |               Login|
    |              Logout|
    |            NextSong|
    |            Register|
    |         Roll Advert|
    |       Save Settings|
    |            Settings|
    |    Submit Downgrade|
    | Submit Registration|
    |      Submit Upgrade|
    |         Thumbs Down|
    +--------------------+
    only showing top 20 rows

For our analyses, we will use the "Cancellation Confirmation" page to
flag when a given customer has churned.

    # create a udf to flag Churned customers
    flag_downgrade_event = udf(lambda x: 1 if x == “Cancellation Confirmation” else 0, IntegerType())

## Data Visualization

We can see some useful insights with the help of graphs. Plotly graphing
options were used during the project. Let's see some interesting plots
that will help us determine the best features for our machine learning
model.

We first check the location distribution. The biggest number of users
are LA residents.

<!-- ![The biggest number of users are LA
residents.](/uploads/blog_images/spark-location-distribution.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-location-distribution.png" alt="Placeholder image" style="width:700px;">
</figure>

It would be more convenient to group the cities into states by creating
a new column that takes the last two characters in the row. After that
manipulation, we can plot the **state distribution.** CA, PA, TX, NH,
and FL are the top five states.

<figure class="image">
<img class="" src="/uploads/blog_images/spark-state-distribution.png" alt="Placeholder image" style="width:700px;">
</figure>
<!-- ![](/uploads/blog_images/spark-state-distribution.png) -->

**What are the most active workdays?**

<!-- ![](/uploads/blog_images/spark-count-workday.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-count-workday.png" alt="Placeholder image" style="width:700px;">
</figure>

**What about churn during the week?**

Friday is the churn day.

<!-- ![](/uploads/blog_images/spark-churn-workday.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-churn-workday.png" alt="Placeholder image" style="width:700px;">
</figure>

**What are the most active hours for users?**

Users are most active in the late afternoon and during the evenings.

<!-- ![](/uploads/blog_images/spark-active-hours.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-active-hours.png" alt="Placeholder image" style="width:700px;">
</figure>

**What are the most active hours for churn then?**

There is something at 10 am, even if it's not the most active time for
users, a lot of churns happen then.

<!-- ![](/uploads/blog_images/spark-active-hours-churn.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-active-hours-churn.png" alt="Placeholder image" style="width:700px;">
</figure>

**What are the most active days of the month?**

The second half of the month tends to be a bit busier, but there is not
a clear pattern.

<!-- ![](/uploads/blog_images/spark-active-days.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-active-days.png" alt="Placeholder image" style="width:700px;">
</figure>

**What are the most active days during the month?**

The beginning and the second half of the month are for churn!

<!-- ![](/uploads/blog_images/spark-active-days-month.png) -->
<figure class="image">
<img class="" src="/uploads/blog_images/spark-active-days-month.png" alt="Placeholder image" style="width:700px;">
</figure>

------------------------------------------------------------------------

## Data Preprocessing

Based on the exploratory analyses we could pick **state, workday, day,
and gender** as our candidate features for modeling. We can also add
**SongsPlayed** to that as it gives an interesting indication --- users
who listen to more songs are less prompt to churn.

    # Get feauture candidates for modeling
    df.select('userId', 'churn', 'gender', 'workday', 'day', 'state') \
        .where(df.churn != 0).sort('userId').show(20)+------+-----+------+-------+---+-----+
    |userId|churn|gender|workday|day|state|
    +------+-----+------+-------+---+-----+
    |    10|    1|     M|      2|  9|   MS|
    |100001|    1|     F|      2|  2|   FL|
    |100003|    1|     F|      4|  8|   FL|
    |100004|    1|     F|      7| 14|   NY|
    |100005|    1|     M|      6|  6|   LA|
    |100010|    1|     F|      4| 11|   CT|
    |100011|    1|     M|      3| 21|   OR|
    |100012|    1|     M|      2|  6|   WI|
    |100013|    1|     F|      2|  2|   OH|
    |100014|    1|     M|      7| 21|   PA|
    |100016|    1|     M|      2| 23|   IL|
    |100017|    1|     M|      2| 13|   AL|
    |100018|    1|     M|      1|  8|   TX|
    |100023|    1|     M|      6|  6|   SC|
    |100024|    1|     M|      2| 13|   PA|
    |100025|    1|     F|      2|  6|   PA|
    |100028|    1|     F|      7| 21|   WA|
    |100030|    1|     F|      3|  3|   CA|
    |100032|    1|     M|      4|  4|   TX|
    |100036|    1|     M|      5|  5|   OK|
    +------+-----+------+-------+---+-----+
    only showing top 20 rows

Spark cannot work with strings when building a model. Furthermore,
proper preprocessing had to be done to make sure that data is feasible
for Spark modeling.

The script used for this process creates a new *model* data frame only
with the columns chosen for features.

-   *userId* and column *gender* are converted to integers

-   *churn* is renamed to *label*

-   new column *SongsPlayed* is created and added to the *model* data
    frame

-   any null values are removed

-   duplicates were also removed

After some data wrangling, we get the below *model* table.

    model.show()+------+-----+------+-------+---+-----+-----------+
    |userId|label|gender|workday|day|state|SongsPlayed|
    +------+-----+------+-------+---+-----+-----------+
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    |100010|    0|     0|      1|  8|   CT|         96|
    |100010|    0|     0|      4| 11|   CT|         96|
    +------+-----+------+-------+---+-----+-----------+
    only showing top 20 rows

One last step is to encode the *state* column as it is a string-type
column with 50+ state names. I used *StringIndexer* and *OneHotEncoder*
to create a vector column of the available state.

    # Create a StringIndexer
    state_indexer = StringIndexer(inputCol =”state”, outputCol =”state_index”)# Create a OneHotEncoder
    state_encoder = OneHotEncoder(inputCol=”state_index”, outputCol=”state_fact”)

The difference between Scikit-learn and Spark machine learning
approaches is the features. In Spark, we should encode all features into
one vectored column. I used *VectorAssembler* to do that.

    # Make a VectorAssembler
    vec_assembler = VectorAssembler(inputCols=[“gender”, “state_fact”, “workday”, “day”, “SongsPlayed”], \
     outputCol=”features”)

------------------------------------------------------------------------

## Implementation

Once all data is good for modeling, the next step is to create a machine
learning pipeline. The pipeline is a class in the pyspark.ml module that
combines all the Estimators and Transformers that I already created.
This lets me reuse the same modeling process over and over again by
wrapping it up in one simple object.

    # Make the pipeline
    churn_pipe = Pipeline(stages=[state_indexer, state_encoder, vec_assembler])

After data is cleaned and gotten ready for modeling, one of the most
vital steps is to split the data into a *test set* and a *train set*.

In Spark, it's important to make sure you split the data **after** all
the transformations. This is because operations like *StringIndexer*
don't always produce the same index even when given the same list of
strings.

    # Fit and transform the data
    piped_data = churn_pipe.fit(model).transform(model)# Split the data into training and test sets
    training, test = piped_data.randomSplit([.6, .4])

For this project, I used Logistic Regression and Random Forest
Classifier to define churn. The very first pick for a classification
model should always be the logistic regression. It's a basic model but
gives a good ground for machine learning predictions.

------------------------------------------------------------------------

## Refinement

I tuned my model using *k-fold cross-validation*. This is a method of
estimating the model's performance on unseen data. It works by splitting
the training data into a few different partitions --- I used Spark's
default values.

Once the data is split up, one of the partitions is set aside, and the
model is fit to the others. Then the error is measured against the
held-out partition. This is repeated for each of the partitions so that
every block of data is held out and used as a test set exactly once.
Then the error on each of the partitions is averaged. This is called the
cross-validation *error* of the model and is a good estimate of the
actual error on the held-out data.

You need to create a grid of values to search over when looking for the
optimal hyperparameters. With the help of cross-validation, I chose the
hyperparameters by creating a grid of the possible pairs of values for
the two hyperparameters, *elasticNetParam* and *regParam*, and using the
cross-validation error to compare all the different models.

The submodule *pyspark.ml.tuning* includes a class called
*ParamGridBuilder* that does just that.You'll need to use the
*.addGrid()* and *.build()* methods to create a grid that you can use
for cross-validation. The *.addGrid()* method takes a model parameter
and a list of values that you want to try. The *.build()* method takes
no arguments, it just returns the grid that I used later.

    # Create the parameter grid
    grid = tune.ParamGridBuilder()
    grid_rf = tune.ParamGridBuilder()# Logistic Regression grid
    grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
    grid = grid.addGrid(lr.elasticNetParam, [0, 1])# Random Forest grid
    grid_rf = grid_rf.addGrid(rf.numTrees,[3, 10, 30])

The sub-module *pyspark.ml.tuning* also has a class called
*CrossValidator* for performing cross-validation.

    # Create the CrossValidator
    cv = tune.CrossValidator(estimator=lr,
                estimatorParamMaps=grid,
                evaluator=evaluator_ROC
                )

The script I built combines all the above chunks into one action that
istailored to suit a Logistic Regression and a Random Forest Classifier.
Once run, the next step is to fit the model and select the best one.
This task takes a lot of time and it depends on the resources in use.

    # Fit cross validation models - this steps takes a lot of time
    models = cv.fit(training)# Extract the best model
    best_lr = models.bestModel

------------------------------------------------------------------------

## Model Evaluation and Validation

To make sure I properly assess the performance of the models I used a
common metric for binary classification algorithms called the ***AUC***
(area under the curve --- numerical representation of the performance of
binary classifier). In this case, the curve is the **ROC** (receiver
operating curve --- the visual representation of the performance of the
binary classifier. False Positive Rate vs True Positive Rate is plotted
to get the visual understanding of the classifier's performance). Both
models were measured against AUC-ROC.

------------------------------------------------------------------------

## Justification

Accuracy is the most common measure but definitely ignores many factors
like false positives and false negatives that are brought into the
system by a model.

To demonstrate why it is important to use the proper metric I will share
the results from my models (random forest performed 23% less compared to
the logistic regression).

**Logistic regression results:**

-   Accuracy = 99%

-   Area under ROC = 65%

**Random Forests results:**

-   Accuracy = 99%

-   Area under ROC = 55%

If I consider only accuracy I would be too confident of my model. Since
I deal with unbalanced data a better estimation of the model's
performance is the Area under ROC, which turns to be 65%.

------------------------------------------------------------------------

## Reflection

Predicting churn is not an easy task but with the help of Spark and
python, it could get really fun. Of course, none of the techniques would
matter if the model that I created is useless for real business.

In my scenario, I used the gender, day, workday, state, and songs played
per user to predict churn. Based on exploratory data analyses I decided
that those features are good for prediction. I found some patterns,
especially for churned users.

Then, I transformed the whole data set to be readable by Spark's machine
learning libraries.I used Logistic Regression and Random Forests to
design a machine learning algorithm.

In the end, I found that Accuracy is a misleading metric for my project,
so I counted on the area under ROC output to measure performance.

------------------------------------------------------------------------

## Improvement

As the famous British statistician George Box stated ***"All models are
wrong but some are useful".***

The AUC-ROC of my models are not so good and I would look further into
the data to choose a different set of features. Maybe a number of
thumbs-up could be a reasonable candidate. There are plenty of
opportunities to test various feature combinations to improve the
model's performance.

I think that we should always link the models to the business context
and make some assumptions about their use. For me, predicting churn was
a challenging task and I would not say that my model is the best
possible solution. However, it gave me a fresh inside and reasonable
confidence that with the right features, I can identify some clients who
are prompt to churn.

------------------------------------------------------------------------

## References

-   <https://towardsdatascience.com/dealing-with-imbalanced-dataset-642a5f6ee297>

-   [https://medium.com/\@sarath13/area-under-the-roc-curve-explained-d056854d3815](https://medium.com/@sarath13/area-under-the-roc-curve-explained-d056854d3815)

-   <https://spark.apache.org/docs/1.5.2/>

-   <https://www.kaggle.com/lpdataninja/machine-learning-with-apache-spark/notebook>

-   [https://www.udacity.com/course/learn-spark-at-udacity--ud2002](https://www.udacity.com/course/learn-spark-at-udacity–ud2002)

------------------------------------------------------------------------
