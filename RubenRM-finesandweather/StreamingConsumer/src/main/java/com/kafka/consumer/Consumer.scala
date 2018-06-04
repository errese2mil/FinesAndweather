package com.kafka.consumer

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.google.gson.Gson
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.math._
import scala.util.matching.Regex
import com.redislabs.provider.redis._
import com.util.Utilidades
import com.beans._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream


object Consumer {

  var BROKERS:String = "localhost:9092"
  var MULTAS_TOPIC:String = "multas"
  var CLIMA_TOPIC:String = "clima"
  var MENSAJES_TOPIC:String = "mensajes"
  var REDIS_HOST:String = "127.0.0.1"
  var REDIS_PORT:String = "6379"


  val APP_NAME:String = "Consumer"
  val KAFKA_VALUE_SERDE:String = "org.apache.kafka.common.serialization.StringSerializer"
  val REDIS_HOST_KEY:String = "redis.host"
  val REDIS_PORT_KEY:String = "redis.port"
  val BROKERS_LIST_KEY:String = "metadata.broker.list"
  val AUTO_OFFSET_RESET_KEY:String = "auto.offset.reset"
  val AUTO_OFFSET_RESET_SMALLEST:String = "smallest"
  val ENABLE_AUTO_COMMIT:String = "enable.auto.commit"
  val TRUE:String = "true"
  val KAFKA_BOOTSTRAP_SERVER:String = "bootstrap.servers"
  val KAFKA_KEY_SERDE:String = "key.serializer"
  val KAFKA_VALUE_KEY:String="value.serializer"
  val CALLEJERO_FILE:String="datasets/callejero/DireccionesVigentes.csv"

  /**
    *
    * @return
    */
  def loadCallejero():Array[(String,String)]={
   sc.textFile(CALLEJERO_FILE)
     .map(line=>line.split(";"))
     .map(arr=>(arr(3),arr(12)))
     .distinct()
     .sortByKey(true)
     .collect()
  }

  /**
    *
    * @param multa
    * @return
    */
  def transformMultas(multa:RDD[(String,String)]): RDD[(String,String)]={
    multa.map(mlIn=>mlIn._2.trim.split(";"))
      .map(mlInArr=>
        Multa(mlInArr(0).trim,
          mlInArr(1).trim,
          mlInArr(2).trim,
          mlInArr(3).trim,
          mlInArr(4).trim,
          mlInArr(5).trim,
          Utilidades.checkDouble(mlInArr(6)),
          mlInArr(7).trim,
          mlInArr(8).trim,
          mlInArr(9).trim,
          mlInArr(10).trim,
          Utilidades.checkInt(mlInArr(11)),
          Utilidades.checkInt(mlInArr(12))
        )  ).map(multaBean=>{
      (findStreet(multaBean.lugar)+"-"+multaBean.dia+"-"+multaBean.mes+"-"+multaBean.anio,
        multaBean.buildMultaValueMessage())
    })
  }

  /**
    *
    * @param clima
    * @return
    */
  def transformClima(clima:RDD[(String,String)]):RDD[(String,String)]={
    clima.map(climaIn=>(climaIn._1,climaIn._2.trim.split(";")))
      .map(climaInArr=>(climaInArr._1,
          Clima(climaInArr._2(0),
            climaInArr._2(3),
            climaInArr._2(2),
            climaInArr._2(1),
            Utilidades.checkDouble(climaInArr._2(5)),
            Utilidades.checkDouble(climaInArr._2(6)),
            Utilidades.checkDouble(climaInArr._2(9)),
            Utilidades.checkDouble(climaInArr._2(10))
          ).buildClimaValueMessage()
      ))
  }

  /**
    *
    * @param multas
    * @return
    */
  def joinRdds(multas:RDD[(String,String)]):RDD[(String,String)]={
    multas.map(multaRDD=>{
      println("buscamos: "+multaRDD._1)
      (multaRDD._1,
        multaRDD._2+sc.fromRedisKV(multaRDD._1)
          .map(multaIn=>multaIn._2)
          .collect()
          .mkString(";"))
    })
  }

  /**
    *
    * @param mensaje
    * @return
    */
  def buildMensaje(mensaje:RDD[(String,String)]):RDD[(String,String)]={
    mensaje.map(mens=>("",mens._2.split(";")))
      .map(mensArr=>{
        if(mensArr._2.length>5)
          (mensArr._1, Mensaje(Utilidades.buildFecha(mensArr._2(0), mensArr._2(1), mensArr._2(2)).toString,
          mensArr._2(1),
          mensArr._2(5),
          Utilidades.checkDouble(mensArr._2(3)),
          mensArr._2(4),
          Utilidades.checkDouble(mensArr._2(6)),
          Utilidades.checkDouble(mensArr._2(7)),
          Utilidades.checkDouble(mensArr._2(8)),
          Utilidades.checkDouble(mensArr._2(9)))
        )
        else (mensArr._1,null)})
      .map(js=>{
        ("", if(js != null && js._2 != null) js._2.transFormToJson() else "")})
  }

  /**
    *
    * @param in
    * @return
    */
  def findStreet(in: String):String={

      var resultAux=("","")
      var dis=in.length*2
      val inFormatted = Utilidades.formatWord(in)
      //println("calle formateada: "+formatWord(in))
      if(callejeroCache.contains(inFormatted)){
        //println("Calle encontrada en cache: " + inFormatted)
        resultAux = callejeroCache.get(inFormatted).get
        //println("Con valor: " + resultAux._1+","+resultAux._2)
      }else{
      callejero.foreach(calle=>{
        var disActual = Utilidades.editDist(inFormatted, calle._1)
        if(disActual < dis){
          dis = disActual
          resultAux = calle
          //println("Calle encontrada: " + calle._1+","+calle._2)
        }
      })
        callejeroCache+=(inFormatted->(resultAux._1,resultAux._2))
    }
    resultAux._2
  }

  /**
    *
    * @param mensajes
    */
  def sendMensajes(mensajes:DStream[(String,String)]):Unit={
    val props = new Properties()
    props.put(KAFKA_BOOTSTRAP_SERVER, BROKERS)
    props.put(KAFKA_KEY_SERDE, KAFKA_VALUE_SERDE)
    props.put(KAFKA_VALUE_KEY, KAFKA_VALUE_SERDE)
    if(mensajes != null) {
      try {
        mensajes.foreachRDD(mensaje => {
          //println("aun pintamos aqui: " + mensaje.collect().mkString("-"))
          mensaje.foreach(mens => {
            println("JSON: " + mens._2)
            val produc: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
            produc.send(new ProducerRecord[String, String](MENSAJES_TOPIC, "", mens._2))
            produc.close()
          })
        })
      } catch {
        case e:Exception => println("ERRORACO")
      }
    }else{
      println("El de mensajes viene vacio")
    }
  }

  //esto ya veremos...de donde lo sacamos
  def loadProperties():Unit={
    BROKERS = "localhost:9092"
    MULTAS_TOPIC = "multas"
    CLIMA_TOPIC = "clima"
    MENSAJES_TOPIC = "mensajes"
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = "6379"
  }

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(APP_NAME)
    .set(REDIS_HOST_KEY,REDIS_HOST)
    .set(REDIS_PORT_KEY,REDIS_PORT)

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc,Seconds(3))
  loadProperties()
  val callejero = loadCallejero()
  var callejeroCache:HashMap[String,Tuple2[String,String]]=new HashMap[String,Tuple2[String,String]]()

  //falta bootstrap del callejero y la cache
  /**
    *
    * @param args
    */
  def main(args: Array[String]):Unit = {

    val kafkaParamsMultas = Map[String, String](
      BROKERS_LIST_KEY -> BROKERS)

    val kafkaParamsClima = Map[String, String](
      BROKERS_LIST_KEY -> BROKERS,
      AUTO_OFFSET_RESET_KEY -> AUTO_OFFSET_RESET_SMALLEST,
      ENABLE_AUTO_COMMIT -> TRUE)

    val multasTopic = Set(MULTAS_TOPIC)
    val climaTopic = Set(CLIMA_TOPIC)

    val climaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParamsClima,climaTopic)

    val climaParsed = climaStream.transform(transformClima(_))

    climaParsed.foreachRDD(rdd=>{
      /*rdd.foreach(x=>{println("GUARDAMOS: "+x)})*/
      sc.toRedisKV(rdd)
     // println("DEBERIA HABER GUARDADO")
    })

    val multasStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParamsMultas,multasTopic)

    val multasParsed= multasStream.transform(transformMultas(_))
    val mergeadas = multasParsed.transform(joinRdds(_))
    //mergeadas.foreachRDD(rdd=>rdd.foreach(x=>{println("JUNTOS :"+x)}))
    val mensajes = mergeadas.transform(buildMensaje(_))

    sendMensajes(mensajes)

    ssc.start()
    ssc.awaitTermination()
  }
}
