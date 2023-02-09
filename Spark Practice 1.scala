// Databricks notebook source
// DBTITLE 1,Importer un fichier RDD
// MAGIC %scala
// MAGIC 
// MAGIC // importer le fichier csv : data_geo.csv
// MAGIC val rdd = sc.textFile("/databricks-datasets/samples/population-vs-price/data_geo.csv")

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC // lister les elements du dossier /databricks/driver/
// MAGIC ls -ltr /databricks/driver/

// COMMAND ----------

// DBTITLE 1,Afficher le contenu du fichier RDD
// MAGIC %scala
// MAGIC 
// MAGIC rdd.collect().foreach(println)

// COMMAND ----------

// DBTITLE 1,Parallelized Collections RDD
// MAGIC %scala
// MAGIC 
// MAGIC // créer un fichier distribué
// MAGIC val data = Array(1, 2, 3, 4, 5)
// MAGIC val distData = sc.parallelize(data)

// COMMAND ----------

// DBTITLE 1,Afficher les collections RDD parallélisées
// MAGIC %scala
// MAGIC 
// MAGIC // collec() est une fonction qui va collecter les données de différents noeuds.
// MAGIC distData.collect().foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC **Attention** : si le fichier est tros volumineux (ex. TB), la fonction collecte() peut renvoyer un message d'erreur "outOfMemory".

// COMMAND ----------

// DBTITLE 1,External Datasets RDD
// MAGIC %scala
// MAGIC 
// MAGIC // importer le fichier 'employee.txt' dans Databricks puis dans Spark
// MAGIC val distFile = sc.textFile("/FileStore/tables/employee.txt")
// MAGIC 
// MAGIC // afficher le dataset (rdd)
// MAGIC distFile.collect().foreach(println)
