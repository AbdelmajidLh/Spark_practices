// -----------------------------------------------------------------
// Importer un fichier csv dans spark
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