package com.kafka.generation


import java.io.File

import scala.io.{Codec, Source}


object Productor{

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //val folder = new File("/home/bigdata/datasets/clima")
    val folder = new File("datasets/clima")
    if(folder.exists() && folder.isDirectory){
      folder.listFiles().toList.sorted.foreach(file=>{
        println("FILE: "+file.getName)

        val clima = Source.fromFile(file)(Codec("utf-8"))
        val climaProducer = new KafkaProductor("localhost:9092","clima")
        val total=20
        var contador=0

        Thread.sleep(2000)

        for(line <- clima.getLines.drop(1)){
          if(total<=contador) {
            Thread.sleep(3000)
            contador=0
          }
          println(line)
          val arr = line.split(";")
          val arrPostalCode = arr(1).split("-")
          val lst = arr.toList.takeRight(arr.length - 2)

          if(lst.size>7) {
            val lst2 = lst.patch(4, Seq(lst(4).split(" ")(0)), 1)
              .patch(5, Seq(lst(5).split(" ")(0)), 1)
              .patch(7, Seq(lst(7).split(" ")(0)), 1)
              .patch(8, Seq(lst(8).split(" ")(0)), 1)
            var newLn: String = ""
            for (pCode <- arrPostalCode) {
              newLn = pCode.concat(";").concat(lst2.mkString(";"))
              //println("mandamos: " + pCode + "-" + arr(4) + "-" + arr(3) + "-" + arr(2)+","+ newLn)
              climaProducer.send(pCode + "-" + arr(4) + "-" + arr(3) + "-" + arr(2), newLn)
            }
          }
          contador = contador + 1
        }
        climaProducer.close()
        clima.close()
      })
    }
  }
}


