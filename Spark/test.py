from pyspark.sql import SparkSession
import sys
inicial = "1"

def appendList(acc, new):
    return acc + "," + new

def colorInicial(k, v):
    if k == inicial:
        return (k, "GRIS|" + v)
    else:
        return (k, "BLANCO|" + v)

def distInicial(k, v):
    if k == inicial:
        return (k,  "0|" + v)
    else:
        return (k, str(sys.maxint) + "|" + v)

def mapBFS(k, v):
    # nodo[0] = dist
    # nodo[1] = color
    # nodo[2] = lista
    nodo = v.split("|", 2)

    if nodo[1] == "GRIS":
        # Recorrer lista de ady
        adyacentes = nodo[2].split(",")
        for ady in adyacentes:
            distAdy = int(nodo[0]) + 1
            nodoNuevo = str(distAdy) + "|GRIS"
            yield (ady, nodoNuevo)
        
        # Colorear nodo negro
        nodoTemp = nodo[0] + "|NEGRO|" + nodo[2]
        yield (k, nodoTemp)
    else:
        yield (k, v)
            
def reduceBFS(acc, new):
    return acc + " " + new

def reduceCaminos(k, v):
    distMin = sys.maxint
    distActual = sys.maxint
    colorFinal = "BLANCO"
    #infoNodo es una lista con dist y la info del nodo
    infoNodo = v.split(" ")
    
    for item in infoNodo:
        if item.count("|") > 1:
            #caso nodo
            temp = item.split("|")
            distActual = int(temp[0])
            colorActual = temp[1]
            listaAdy = temp[2]

        else:
            #caso distancia
            distYColor = item.split("|")
            dist = int(distYColor[0])
            colorActual = distYColor[1]

            if (dist < distMin):
                distMin = dist
        
        if (distMin < distActual):
            distActual = distMin
        
        # Comparar colores
        if colorActual == "NEGRO":
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

    # Fase 1: Tomar el input y convertirlo al formato (userid, dist|color|list)
    # Falta: pedir nodo inicial por arg
    #       dist 0 para nodo inicial 
    lines = spark.read.text("input_small.txt")
    pairs = lines.rdd.map(lambda l: l.value.split("    "))
    listaFinal = pairs.reduceByKey(appendList).map(lambda (k, v): colorInicial(k, v)).map(lambda (k, v): distInicial(k, v))
    #print(listaFinal.collect())

    
    # FASE 2: BFS
    listMap = listaFinal.map(lambda (k, v): list(mapBFS(k, v))).flatMap(lambda l: l)
    #print(listMap.collect())
    listReduce = listMap.reduceByKey(reduceBFS)
    listaBFS = listReduce.map(lambda (k, v): reduceCaminos(k, v))
    print(listaBFS.collect())

    spark.stop()