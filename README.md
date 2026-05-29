# SPARQL2FlinkHDT

SPARQL2FlinkHDT es una extensión de la librería [SPARQL2Flink](https://github.com/oscarceballos/sparql2flink), desarrollada por el doctor Oscar Ceballos Argote para el procesamiento distribuido de consultas SPARQL sobre grandes conjuntos de datos RDF utilizando Apache Flink.

Este proyecto surge como resultado de una investigación de maestría orientada a evaluar el impacto de incorporar una técnica de serialización RDF basada en **HDT (Header, Dictionary, Triples)** dentro del proceso de ejecución de consultas SPARQL en entornos distribuidos.

## Motivación

El procesamiento de grandes RDF datasets representa uno de los principales desafíos en aplicaciones de Big Data y Web Semántica. Aunque SPARQL2Flink permite ejecutar consultas SPARQL de forma distribuida sobre Apache Flink, su arquitectura original trabaja sobre datasets RDF en formato N-Triples, lo que puede generar altos tiempos de carga y elevado consumo de memoria.

HDT es un formato compacto para la representación de datos RDF que utiliza diccionarios e identificadores numéricos para reducir el espacio requerido por los triples. Este proyecto explora la integración de dicha técnica dentro de SPARQL2Flink con el fin de evaluar sus efectos sobre el consumo de recursos, la escalabilidad y el desempeño general del sistema.

## Descripción

SPARQL2FlinkHDT modifica el flujo de procesamiento de SPARQL2Flink para permitir que las consultas SPARQL sean ejecutadas sobre representaciones internas basadas en HDT.

Para ello, se incorporan mecanismos que permiten:

- Convertir RDF datasets desde N-Triples hacia HDT.
- Gestionar diccionarios serializables compatibles con Apache Flink.
- Ejecutar operaciones de filtrado, unión y proyección utilizando identificadores numéricos.
- Convertir los resultados obtenidos nuevamente a representaciones RDF legibles.
- Reducir el consumo de memoria requerido para procesar datasets RDF de mayor tamaño.

## Resultados obtenidos

Los experimentos realizados sobre un clúster Apache Flink mostraron que la integración de HDT introduce una sobrecarga adicional asociada a la gestión del diccionario y a la conversión entre identificadores y recursos RDF. Como consecuencia, los tiempos de carga y ejecución fueron mayores que los obtenidos con la versión original de SPARQL2Flink.

Sin embargo, la utilización de HDT permitió procesar volúmenes de datos significativamente mayores sin presentar errores de memoria, incrementando la capacidad de procesamiento del sistema y mejorando su escalabilidad en términos del tamaño de dataset soportado.

Los resultados evidencian un compromiso (*trade-off*) entre tiempo de ejecución y capacidad de procesamiento, aportando evidencia sobre el comportamiento de técnicas de compresión RDF en entornos distribuidos basados en Apache Flink.

## Arquitectura general

La arquitectura implementada conserva la estructura modular de SPARQL2Flink, compuesta principalmente por los módulos `mapper` y `runner`.

- El módulo `mapper` transforma una consulta SPARQL en un programa ejecutable sobre Apache Flink.
- El módulo `runner` ejecuta el programa generado, carga el RDF dataset, realiza la conversión hacia HDT y gestiona las estructuras necesarias para el procesamiento distribuido.

Entre los componentes incorporados o adaptados se encuentran:

- `SerializableDictionary`: representación serializable del diccionario HDT.
- `TripleIDConvert`: conversión entre URIs/literales e identificadores HDT.
- `SolutionMappingHDT`: representación de soluciones parciales basadas en identificadores.
- `TripleID2TripleString`: conversión de resultados codificados hacia representaciones RDF legibles.
- Adaptaciones de operadores como `Triple2Triple`, `Join`, `LeftJoin`, `Filter` y `Project`.

## Requisitos

- Java 11
- Apache Maven
- Apache Flink 1.15.0
- Apache Hadoop 3.3.6
- Apache Jena 4.0.0
- HDT Java

