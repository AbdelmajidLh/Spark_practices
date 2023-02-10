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
// Data exploration - utilisation de l'API Spark DataFrames
// -----------------------------------------------------------------
// importer le dataframe en utilisant un schema explicite : 
// spécifier chaque schema individuellement dans le code (vs inferschema)
%scala

// importer la librairie Encoders
import org.apache.spark.sql.Encoders;

// creer une case class : railway et specifier les types de colonnes
case class railway(DayofMonth:Int, DayofWeek:Int, State:String, OriginStationID:Double, DestStationID:Int, DepDelay:Int, ArrDelay:Int)

// créer le schema
val railwaySchema = Encoders.product[railway].schema

// créer un dataframe
val railwayDF = spark.read.schema(railwaySchema).option("header", "true")
                                                .csv("/FileStore/tables/railway.csv")
// afficher le df importé : 10 lignes
railwayDF.show(10)

// afficher le schema
railwayDF.printSchema()


// [not run] importer le fichier en inferant le schema : pas bon en production : si les données changent
// val railwayDF = spark.read.option("inferSchema", "true")
//                          .option("header", "true")
//                          .csv("/FileStore/tables/railway.csv")


// Utiliser les méthodes de DataFrame dans spark
// selectionner la colonne State
val states = railwayDF.select("State")
states.show(15)

// compter les lignes dans le df
railwayDF.count()

// summary statistics du df : la fonction retourne un df. Utiliser show pour l'afficher
railwayDF.describe().show()

// -----------------------------------------------------------------
// utilisation de Spark SQL
// -----------------------------------------------------------------

