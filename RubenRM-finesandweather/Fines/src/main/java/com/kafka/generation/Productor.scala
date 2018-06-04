package com.kafka.generation

import java.io.File
import scala.io.{Codec, Source}


object Productor{

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //val folder = new File("/home/bigdata/datasets/multas/")
    val folder = new File("datasets/multas/")
    if(folder.exists() && folder.isDirectory) {
      folder.listFiles().toList.sorted.foreach(file =>{

        val multas = Source.fromFile(file)(Codec("utf-8"))
        val multasProducer = new KafkaProductor("localhost:9092","multas")

        var contador=0
        for(line <- multas.getLines.drop(1)){

          if(contador==500) {
            println("mandamos: "+line.trim)
            multasProducer.send(line.trim)
            contador=0
            Thread.sleep(500)
          }
          contador = contador+1

        }

        multasProducer.close()
        multas.close()

      })
    }
  }
}


