# Projet-Big-Data
Ce projet traite de l'analyse des données méréologiques depuis un Dataset d'une ville aux Etats-Unis.

I-	Objectif 

L’objectif de ce projet est d’obtenir une application Java utilisant la technlogie Map Reduce dans le but d’analyser les données météorologique provenant d’un dataset. Notre groupe de dataset contient des données de plusieurs régions du monde sur plusieurs années. 

Nous avons décidé de nous focaliser sur un seul dataset qui concerne donc les données météorologiques de la Ville de Fairbanks en Alaska aux Etats-Unis durant l’année 2020.

Les données de notre Dataset sont sous format ASCII avec chaque ligne contenant plusieurs informations comme la longitude, la latitude, la température maximale de la journée, la température minimale de la journée ou encore la température moyenne.


II-	Description du code 


Nous avons dans un premier temps importer toutes les classes nécessaires à notre algorithme.
A la suite de cela nous implémentons 2 classes qui extendent les super classes Mapper et Reducer.

•	Mappeur 

On rappel que la fonction Map transforme les entrées du disque en paires (clé,valeur) les traite et génère un autre ensemble de paires (clé,valeur) intermédiaires en sortie.

•	Reducer
Nous utilisons ensuite la méthode du Reducer pour avoir un ensemble de pairs (clé, valeurs).


Nous passons enfin à l’étape job pour planifier notre éxecution Map Reduce.

Pour effectuer cela, nous envoyons 3 classes à notre fonction Jar :
-	La classe MaxMin qui est notre classe principale,
-	Les classes MapperClass et ReducerClass pour effectuer l’algorithme

On effectue un Job en lui donnant les fichiers à traiter et à fournir 

•	HDFS
Tout d’abord on va importer notre dataset dans HDFS à l’aide de la commande hadoop fs -copyFromLocal 

Ensuite, on a donc cherché à avoir notre fichier qui nous fournirait les jours les plus chauds et les plus froids de notre dataset à l’aide de la commande hadoop jar.

