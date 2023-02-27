// ----------------------------------------------------
// IMPORTER LES DONNEES DANS APACHE SPARK
// ----------------------------------------------------

// à partir d'un fichier csv
// infer schema pour prédire le type de colonnes

val sales = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/100Records.csv")

  sales.printSchema()
display(sales)

// ----------------------------------------------------
// Vérifier et controler la donnée dans Spark
// ----------------------------------------------------
// Permissive : Par défaut : les champs où on arrive pas à identifier leur contenu sont remplacés par NaN
// DROPMALFORMED :les lignes non reconnues sont supprimées
// FAILFAST : annuler l'importation si un format non correcte est rencontré
import org.apache.spark.sql.types._
val schema = new StructType()
    .add("Region", StringType, true) // true : accepte missing values
    .add("Country", StringType, true)
    .add("ItemType", StringType, true)
    .add("SalesChannel", StringType, true)
    .add("OrderPriority", StringType, true)
    .add("OrderDate", StringType, true)
    .add("OrderID", IntegerType, true)
    .add("ShipDate", StringType, true)
    .add("UnitsSold", IntegerType, true)
    .add("UnitPrice", DoubleType, true)
    .add("UnitCost", DoubleType, true)
    .add("TotalRevenue", DoubleType, true)
    .add("TotalCost", DoubleType, true)
    .add("TotalProfit", DoubleType, true)

val sales_with_schema = spark.read.format("csv")
    .option("header", "true")
    .oprtion("mode", "PERMISSIVE")
    .schema(schema)
    .load("/FileStore/tables/100_Sales_Records-1.csv")

    // afficher le dataset
display(sales_with_schema)

// Utiliser SQL pour créer un temp View
%sql
-- le mode "FAILFAST" va annuler l'importation du fichier si le format n'est pas respecté
CREATE TEMPORARY VIEW salesview
USING csv
OPTIONS (path "/FileStore/tables/100_Sales_Records-1.csv", header "true", mode "FAILFAST")

%sqlselect * from salesview



// à partir d'un fichier json
%scala 
val df = spark.read.json("/FileStore/tables/example_1.json")
display(df)
df.printSChema()

// specifier le schema manuellement
import org.apache.spark.sql.types._
val schema = new StructType()
    .add("color", StringType, true)
    .add("fruit", StringType, true)
    .add("size", StringType, true)

val df_with_schama = spark.read.schema(schema).json("/FileStore/tables/example_1.json")
display(df_with_schama)

