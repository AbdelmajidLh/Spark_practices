// ############################################################
// Les bases de Spark - scala
// ############################################################
// Ce fichier a été testé sous Notebook Databricks avec scala V2.

// -----------------------------------------------------------------
// Importer un fichier RDD
// -----------------------------------------------------------------


// %scala
//      importer le fichier csv : data_geo.csv
val rdd = sc.textFile("/databricks-datasets/samples/population-vs-price/data_geo.csv")

// %sh
// lister les elements du dossier /databricks/driver/
ls -ltr /databricks/driver/

// Afficher le contenu du fichier RDD
// %scala
rdd.collect().foreach(println)

// Parallelized Collections RDD
// %scala
// créer un fichier distribué
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)


// Afficher les collections RDD parallélisées
// %scala
// collec() est une fonction qui va collecter les données de différents noeuds.
distData.collect().foreach(println)

// %md
// **Attention** : si le fichier est tros volumineux (ex. TB), la fonction collecte() peut renvoyer un message d'erreur "outOfMemory".

// External Datasets RDD
// %scala
// importer le fichier 'employee.txt' dans Databricks puis dans Spark
val distFile = sc.textFile("/FileStore/tables/employee.txt")

// afficher le dataset (rdd)
distFile.collect().foreach(println)
