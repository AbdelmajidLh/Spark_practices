
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

