// -----------------------------------------------------------------
// Importer un fichier csv dans spark
// -----------------------------------------------------------------
%scala

// importer le fichier 100Records.csv
val employeeDF = spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/100Records.csv")

// afficher le schema du distFile
employeeDF.printSchema()

// Créer un TempView (view temporaire) pour lancer des requêtes SQL
%scala
employeeDF.createOrReplaceTempView("employee")

// Lancer les requêtes SQL
%sql

// selectionner tous les elements du df
select * from employee

