
# -*- coding: utf-8 -*-
#########################################################################################
# CI5312 - Gestión de Grandes Volúmenes de Datos
# Implementación de BFS en PySpark
# Adolfo Jeritson
# Carlos Vazquez
# Jose Palma

# #################################### EJECUCION ########################################
# python bfs.py <archivo de entrada> <nodo inicial>
# spark-submit --master local[4] bfs.py <archivo entrada> <nodo inicial>
#########################################################################################
from pyspark.sql import SparkSession
import sys, argparse, hashlib

# Función para hacer reduce y acumular la lista de adyacencias
def appendList(acc, new):
    followers = acc.split(",")
    followers.append(new)
    return ",".join(followers)

# Función para asignar el color y distancia inicial a los nodos
def datosIniciales(k, v):
    if k == args.nodo:
        return (k, "0|GRIS|" + v)
    else:
        return (k, str(sys.maxint) + "|BLANCO|" + v)

# Función que explora la lista de adyacencia de los nodos color GRIS
def mapBFS(k, v):
    nodo = v.split("|")

    # Si el nodo es GRIS, se explora
    if nodo[1] == "GRIS":
        # Recorrer lista de adyacencias
        adyacentes = nodo[2].split(",")
        for ady in adyacentes:
            if ady != "":
                # Calcular distancia y armar información
                distAdy = int(nodo[0]) + 1
                nodoNuevo = str(distAdy) + "|GRIS"
                yield (ady, nodoNuevo)
        
        # Colorear nodo negro
        nodoTemp = nodo[0] + "|NEGRO|" + nodo[2]
        yield (k, nodoTemp)

    # Si no, devolver datos del nodo sin modificaciones
    else:
        yield (k, v)

# Función que acumula las distancias y la información del nodo
def reduceBFS(acc, new):
    datos = acc.split(" ")
    datos.append(new)
    return " ".join(datos)

# Función que recorre todas las distancias encontradas para el nodo
# y que determina el camino más corto
def reduceCaminos(k, v):
    distMin = sys.maxint
    distActual = sys.maxint
    colorFinal = "BLANCO"
    listaAdy = ""

    #infoNodo es una lista con distancia y la información del nodo
    infoNodo = v.split(" ")
    
    for item in infoNodo:
        if item.count("|") > 1:
            # Caso información del nodo
            temp = item.split("|")
            distActual = int(temp[0])
            colorActual = temp[1]
            listaAdy = temp[2]

        else:
            # Caso distancia posible
            distYColor = item.split("|")
            dist = int(distYColor[0])
            colorActual = distYColor[1]

            if (dist < distMin):
                distMin = dist
        
        if (distMin < distActual):
            distActual = distMin
        
        # Comparar colores y guardar el más oscuro
        if colorActual == "NEGRO" or colorFinal == "NEGRO":
            colorFinal = "NEGRO"
        elif colorActual == "GRIS" and colorFinal == "BLANCO":
            colorFinal = "GRIS"
        elif colorActual == "BLANCO"  and colorFinal == "BLANCO":
            colorFinal = "BLANCO"
        else:
            colorFinal = "GRIS"
    
    nodoNuevo = str(distActual) + "|" + colorFinal + "|" + listaAdy
    return (k, nodoNuevo)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BFS").getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Archivo de entrada.")
    parser.add_argument("nodo", help="ID nodo inicial.")
    args = parser.parse_args()

    print("##################### ENTRANDO FASE 1")
    # Fase 1: Tomar el input y convertirlo al formato (userid, dist|color|list)
    lines = spark.read.text(args.input)
    pairs = lines.rdd.map(lambda l: l.value.split("\t"))
    pairs.cache()
    listaFinal = pairs.reduceByKey(appendList).map(lambda (k, v): datosIniciales(k, v))
    listaFinal.cache()
    print("##################### SALIENDO FASE 1")
    #listaFinal.saveAsTextFile("output-test")
    #sys.exit()

    print("##################### ENTRANDO FASE 2")
    # FASE 2: BFS
    parada = False
    iteracion = 0

    while not parada:
        listMap = listaFinal.map(lambda (k, v): list(mapBFS(k, v))).flatMap(lambda l: l)
        listReduce = listMap.reduceByKey(reduceBFS)
        listaBFS = listReduce.map(lambda (k, v): reduceCaminos(k, v))
        #listaBFS.cache()
        #listaBFS.coalesce(1)
        #listaFinal.coalesce(1)

        #hash1 = hashlib.md5()
        #hash1.update(str(listaFinal.collect()))
        #hash2 = hashlib.md5()
        #hash2.update(str(listaBFS.collect()))
        #um_negros_viejo = listaFinal.filter(lambda (k, v): ("NEGRO" in v)).count()
        #num_negros_nuevo = listaBFS.filter(lambda (k, v): ("NEGRO" in v)).count()
        num_gris = listaBFS.filter(lambda (k, v): "GRIS" in v).count()

        if (num_gris == 0):
            parada = True
        else:
            listaFinal = listaBFS
        
        #listaFinal.cache()
        #print(" ")
        #print(listaBFS.collect())
        #print(" ")
        print
        print("######################## ITERACIÓN: #"+ str(iteracion))
        print
        iteracion += 1
    
    #listaBFS.write.text("output-bfs.txt")
    listaBFS.saveAsTextFile("output-bfs")

    spark.stop()
