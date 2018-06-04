package com.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.beans.Mensaje
import com.google.gson.Gson

import scala.math.min
import scala.util.matching.Regex

object Utilidades {

  /**
    *
    * @param in
    * @return
    */
  def formatWord(in:String):String={
    var inTrimed = in.trim
    if(inTrimed.indexOf(" - ")> -1){
      inTrimed = inTrimed.substring(0,inTrimed.indexOf(" - "))
    }
    val stop = List("S/N","AV","-","CR","GT","KM","CL","P.","_","CALLE")
    var strSplited= inTrimed.split(" ").toList
    val parsedStr = strSplited.filter(!stop.contains(_)).mkString(" ")

    val pattern = new Regex("[a-zA-Z]")
    pattern.findAllIn(parsedStr).mkString
  }

  def editDist[A](a: Iterable[A], b: Iterable[A]) ={
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      })last
  }

  /**
    *
    * @param in
    * @return
    */
  def checkDouble(in:String):Double={
    if(in.trim.equals("")) 0.0 else in.trim.toDouble
  }

  /**
    *
    * @param in
    * @return
    */
  def checkInt(in:String):Int={
    if(in.trim.equals("")) 0 else in.trim.toInt
  }

  /**
    *
    * @param anio
    * @param mes
    * @param dia
    * @return
    */
  def buildFecha(anio:String,mes:String,dia:String):Long={

    if(!"".equals(anio) && !"".equals(mes) && !"".equals(dia)){
      val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val dfNew:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00'Z'")
      df.parse(anio+"-"+mes+"-"+dia).getTime()
    }else{
      throw new Exception("Fecha vacia")
    }
  }
}
