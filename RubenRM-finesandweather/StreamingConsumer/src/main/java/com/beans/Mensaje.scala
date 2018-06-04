package com.beans

import com.google.gson.Gson

/**
  *
  * @param fecha
  * @param cp
  * @param importe
  * @param puntos
  * @param maxTemp
  * @param minTemp
  * @param viento
  * @param lluvia
  */
case class Mensaje(fecha:String,
                   mes:String,
                   cp:String,
                   importe:Double,
                   puntos:String,
                   maxTemp:Double,
                   minTemp:Double,
                   viento:Double,
                   lluvia:Double){

  /**
    *
    *
    * @return
    */
  def transFormToJson():String={
    if(this != null) {
      val g: Gson = new Gson()
      g.toJson(this)
    }else{""}
  }

}
