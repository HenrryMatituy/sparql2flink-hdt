# SPARQL2FlinkHDT

La librería [SPARQL2Flink](https://github.com/oscarceballos/sparql2flink), desarrollada 
por el doctor Oscar Ceballos Argote, permite el procesamiento de consultas SPARQL sobre grandes conjuntos de 
datos  RDF en un clúster Apache Flink. Previamente, se desarrolló una prueba de concepto sobre SPARQL2Flink, 
realizando una actualización de la librería, a la que se denominó [SPARQL2Flink-hdt](https://github.
com/oscarceballos/sparql2flink-hdt.git). En esta nueva propuesta se logró realizar el procesamiento de consultas SPARQL 
sobre la librería original pero esta vez utilizando la técnica de serialización HDT. Sin embargo...

Este proyecto toma como base la prueba de concepto mencionada anteriormente, con el objetivo de extender la 
librería SPARQL2Flink con una técnica de serialización basada en HDT con el fin de reducir el 
tiempo de carga del RDF dataset en memoria RAM y los tiempos de respuesta de una consulta SPARQL.


[//]: # (## References)

[//]: # (O. Ceballos, C. A. R. Restrepo, M. C. Pabón, A. M. Castillo, and O. Corcho, “Sparql2flink: Evaluation of sparql queries on apache flink,” Applied Sciences &#40;Switzerland&#41;, vol. 11, no. 15, 2021.)

[//]: # ()
[//]: # (O. Ceballos, “Sparql2flink library,” https://github.com/oscarceballos/sparql2flink, mar 2018,)

[//]: # ([Online; accessed March 24, 2020]. [Online]. Available: https://github.com/oscarceballos/sparql2flink)