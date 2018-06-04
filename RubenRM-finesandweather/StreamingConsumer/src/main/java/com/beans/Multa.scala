package com.beans

/**
  *
  * @param calificacion
  * @param lugar
  * @param dia
  * @param mes
  * @param anio
  * @param hora
  * @param importe
  * @param descuento
  * @param puntos
  * @param denunciante
  * @param hecho
  * @param limite
  * @param velocidad
  */
case class Multa(calificacion:String,
             lugar:String,
             dia:String,
             mes:String,
             anio:String,
             hora:String,
             importe:Double,
             descuento:String,
             puntos:String,
             denunciante:String,
             hecho:String,
             limite:Int,
             velocidad:Int){

  /**
    *
    * @return
    */
  def buildMultaValueMessage():String={
    val resultado = StringBuilder.newBuilder
    resultado.append(anio)
    resultado.append(";")
    resultado.append(mes)
    resultado.append(";")
    resultado.append(dia)
    resultado.append(";")
    resultado.append(importe)
    resultado.append(";")
    resultado.append(puntos)
    resultado.append(";")
    resultado.toString()
  }

}
