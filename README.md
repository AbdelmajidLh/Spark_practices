# Les bases de Spark - scala
## Contexte


Dans ce dépôt, j'ai mis des exemples pratiques des commandes Apache Spark avec Scala. Le but est de vous montrer comment créer, importer et manipuler des fichiers rdd et datasets avec scala dans un environnement Spark ([Databricks](https://community.cloud.databricks.com/)).

Les fichiers avec extention `/databricks_files/*.dbc` sont des fichiers databricks que vous pouvez réutiliser, sinon, vous pouvez utiliser les fichier ``.scala`` diréctement.

## Scala vs Python pour Apache Spark
Apache Spark, le célèbre framework d’analyse Big Data, est écrit en Scala. C’est ce qui lui permet d’offrir une vitesse élevée grâce à sa nature statique. Toutefois, Spark propose des APIs pour Scala, Python, Java et R. Les deux langages les plus utilisés pour Spark sont Scala et Python.

En termes de performances, Scala est dix fois plus rapide que Python. Ce langage utilise Java Virtual Machines pendant le runtime, ce qui lui offre une vitesse accrue dans la plupart des cas. La nature dynamique de Python réduit aussi sa vitesse.

Les bibliothèques Spark doivent être appelées par Python, et ceci requiert beaucoup de traitement de code. Dans ce cas de figure, Scala fonctionne bien avec un nombre de coeurs limité.

De plus, Scala interagit mieux avec les services Hadoop et notamment le système de fichiers HDFS sur lequel est basé Spark. Avec Python, les développeurs doivent utiliser des bibliothèques tierces comme Hadoopy, alors que Scala interagit avec Hadoop via des API natives en Java. Il est donc plus facile d’écrire des applications Hadoop natives en Scala.

## Sources
Les exemples sont issus de la formation [Machine Learning with Apache Spark 3.0 using Scala]() que j'ai suivi sur Udemy.
Comparaison Scala vs Python : [Datascientest](https://datascientest.com/scala#:~:text=Le%20langage%20Scala%20est%20un,le%20distingue%20des%20autres%20langages.)