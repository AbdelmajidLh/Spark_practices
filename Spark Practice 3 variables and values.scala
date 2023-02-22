
// -----------------------------------------------------------------
// Les valeurs dans scala
// -----------------------------------------------------------------
%scala

// ecrire une valeur/variable ajout 1
val addOne = (x: Int) => x + 1
println(addOne(1)) // 2

// somme de deux variables
val add = (x: Int, y: Int) => x + y
println(add(1, 2)) // 3

// fonction sans paramètres
val getTheAnswer = () => 42
println(getTheAnswer()) // 42

// -----------------------------------------------------------------
// Les Datasets dans scala
// -----------------------------------------------------------------
// un dataset est une combinaison de RDD et dataframes
// Créer une liste avec la fonction range
val range20 = spark.range(20)
range20.collect().foreach(println)

// Créer un dataset dans spark
val dataset = spark.createDataFrame(Seq(
  (1, "John", "Smith", 20, "Male"),
  (2, "Jane", "Doe", 21, "Female"),
  (3, "Max", "Williams", 22, "Male"),
  (4, "Amy", "Jones", 23, "Female"),
  (5, "David", "Brown", 24, "Male"),
  (6, "Sara", "Miller", 25, "Female"),
  (7, "Roger", "Wilson", 26, "Male"),
  (8, "Karen", "Taylor", 27, "Female"),
  (9, "Steve", "Harris", 28, "Male"),
  (10, "Emily", "Clark", 29, "Female"),
  (11, "Dan", "Lee", 30, "Male"),
  (12, "Kathy", "Robinson", 31, "Female"),
  (13, "Mike", "Martin", 32, "Male"),
  (14, "Alice", "Thompson", 33, "Female"),
  (15, "Brian", "Walker", 34, "Male"),
  (16, "Linda", "Adams", 35, "Female"),
  (17, "George", "Baker", 36, "Male"),
  (18, "Julie", "Garcia", 37, "Female"),
  (19, "Ronald", "Nelson", 38, "Male"),
  (20, "Susan", "Edwards", 39, "Female")
)).toDF("ID", "FirstName", "LastName", "Age", "Gender")

// afficher le dataset
dataset.show()
display(dataset)

// filtrer les hommes agés de 25 an ou plus
val filteredDataset = dataset.filter($"Gender" === "Male" && $"Age" >= 25)
display(filteredDataset)

// **** Importer un fichier json ****
// Tout d'abord, définissez une case class qui représente un objet JVM Scala spécifique au type.
// case class Employee (EmpID: Long, FirstName: String, LastName: String, Gender: String, EMail: String) 
// Lire le fichier JSON, convertir les DataFrames en un objet JVM Scala spécifique au type.
// Employé. A ce stade, Spark, en lisant le JSON, a créé un objet générique
// DataFrame = Dataset[Rows]. En convertissant explicitement DataFrame en Dataset
// on obtient un type spécifique de rangées ou de collection d'objets de type Employee (shema de class) 
val ds = spark.read.json("/FileStore/tables/employee.json").as[Employee]

// afficher le dataset
display(ds)
ds.show()
ds.take(10).foreach(println)

// Manipuler et visualiser les datasets 
// Filtrer le dataset sur le Gender M et selectionner 3 colonnes
//      dataset filter
val dsTemp = ds.filter(d => {d.Gender == "M"}).map(d => (d.EmpID, d.FirstName, d.LastName))display(ds.select)
display(dsTemp)

//      sql filter : select
display(ds.select($"EmpID", $"FirstName", $"LastName").where($"Gender" === "F").sort($"FirstName")