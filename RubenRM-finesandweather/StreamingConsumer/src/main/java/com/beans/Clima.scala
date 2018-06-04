package com.beans

/**
  *
  * @param cp
  * @param dia
  * @param mes
  * @param anio
  * @param maxTemp
  * @param minTemp
  * @param viento
  * @param lluvia
  */
case class Clima(cp:String,
                 dia:String,
                 mes:String,
                 anio:String,
                 maxTemp:Double,
                 minTemp:Double,
                 viento:Double,
                 lluvia:Double){


  /**
    *
    * @return
    */
  def buildClimaValueMessage():String={
    val resultado = StringBuilder.newBuilder
    resultado.append(cp)
    resultado.append(";")
    resultado.append(maxTemp)
    resultado.append(";")
    resultado.append(minTemp)
    resultado.append(";")
    resultado.append(viento)
    resultado.append(";")
    resultado.append(lluvia)
    resultado.append(";")
    resultado.toString()
  }

}
