// -----------------------------------------------------------------
// Importer un fichier csv dans spark, utiliser sql pour 
// filtrer et visualiser les données
// -----------------------------------------------------------------
%scala

// importer le fichier 100Records.csv
val employeeDF = spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/100Records.csv")

// compter le nombre de lignes
employeeDF.count()


// afficher le contenu du df
employeeDF.show()


// afficher le schema du distFile
employeeDF.printSchema()

// Créer un TempView (view temporaire) pour lancer des requêtes SQL
%scala
employeeDF.createOrReplaceTempView("employee")

// Lancer les requêtes SQL
%sql

// selectionner tous les elements du df
select * from employee

// Visualiser le DataFrame
%sql

select count(State), State from employee group by State order by count(State) desc


// -----------------------------------------------------------------
// Utiliser Spark SQL pour analyser les df
// -----------------------------------------------------------------
// importer le csv
val ItemDataframe = sqlContext.read.format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load("/FileStore/tables/Item.csv")

// specifier les colonnes à importer
ItemDataframe.select("ItemName", "Price")

// filtrer les prix > 10
ItemDataframe.filter("Price" > 10)

// créer un TempView
ItemDataframe.createOrReplaceTempView("ItemDataframe")

// utiliser spark.sql pour selectionner les données
ItemDataframe = spark.sql('select * from ItemDataframe')

// aficher l'histograme avec sql : notebnook
%sql
select * from ItemDataframe

// -----------------------------------------------------------------
// Pratique - utilisation de l'API dataframe
// -----------------------------------------------------------------
