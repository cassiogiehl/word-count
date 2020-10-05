package com.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs:///tmp/shakespare.txt").collect()

    // removendo caracteres que não estou interessado em contar
    val text = rdd.map(words => words.replace(".", "")
                                      .replace(",", "")
                                      .replace("!", "")
                                      .replace("?", "")
                                      .replace(";", "")
                                      .replace(":", "")
                                      .replace("'", "")
                                      .replace("#", "")
                                      .replace("[", "")
                                      .replace("]", "")
                                      .replace("*", "")
                                      .replace("&", "")
                                      .replace("(", "")
                                      .replace(")", "")
                                      .replace("\"", "")
                                      .toLowerCase)

    // removendo linhas vazias
    val lines = text.filter(line => line.length > 0)

    // obtendo palavras e criando contador que vai ser acumulado posteriormente
    val words = lines.flatMap(word => word.split(" "))
                     .map(word => (word, 1))

    // acumulando a quantidade de ocorrências de cada palavra
    val wordcount = sc.parallelize(words)
                      .reduceByKey((acum, b) => acum + b)
                      .sortByKey(false)

    wordcount.collect()

//    salvando no formato de arquivos do hadoop (hdfs)
    wordcount.saveAsTextFile("hdfs:///tmp/wordcount")
  }
}
