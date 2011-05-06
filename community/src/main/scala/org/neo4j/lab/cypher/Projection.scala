package org.neo4j.lab.cypher

import org.neo4j.lab.cypher.pipes.Pipe
import org.neo4j.graphdb.NotFoundException
import scala.collection.JavaConverters._
import org.apache.commons.lang.StringUtils


/**
 * Created by Andres Taylor
 * Date: 4/19/11
 * Time: 19:18 
 */

class Projection(source: Pipe, transformers: Seq[Map[String, Any] => Map[String, Any]]) extends Traversable[Map[String, Any]] {
  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach((m) => {
      val result = transformers.map((transformer) => transformer.apply(m)).reduceLeft(_ ++ _)
      f.apply(result)
    })
  }

  def columnAs[T](column: String): Iterator[T] = {
    this.map((map) => {
      val item: Any = map.getOrElse(column, throw new NotFoundException())
      item.asInstanceOf[T]
    }).toIterator
  }

  def javaIterator: java.util.Iterator[java.util.Map[String, Any]] = this.map((m) => m.asJava).toIterator.asJava

  //TODO: This is horrible. The Result object must know it's metadata in a state that doesn't force us to loop
  def calculateColumnSizes: Map[String, Int] = {
    val columnSizes = new scala.collection.mutable.HashMap[String, Int]

    this.foreach((m) => {
      m.foreach((kv) => {
        val length = kv._2.toString.size
        if (!columnSizes.contains(kv._1) || columnSizes.get(kv._1).get < length) {
          columnSizes.put(kv._1, length)
        }
      })
    })
    columnSizes.toMap
  }

  override def toString(): String = {
    val columnSizes = calculateColumnSizes
    val columns = columnSizes.keys.toList
    val builder = new StringBuilder()

    val headers = columns.map((c) => Map[String, Any](c -> c)).reduceLeft(_ ++ _)
    builder.append( "| " + createString(columns, columnSizes, headers) + " |"+"\r\n")
    val wholeLine = StringUtils.repeat("-", builder.length - 2)
    builder.append(wholeLine+"\r\n")
    builder.insert(0,wholeLine+"\r\n")

    foreach( (m) => {
      builder.append( "| " + createString(columns, columnSizes, m) + " |"+"\r\n")
    })

    builder.append(wholeLine+"\r\n")

    builder.toString()
  }

  def createString(columns: List[String], columnSizes: Map[String, Int], m: Map[String, Any]): String = {
    columns.map((c) => {
      val length = columnSizes.get(c).get
      val value = StringUtils.rightPad(StringUtils.left(m.get(c).get.toString, length), length)
      value
    }).reduceLeft(_ + " | " + _)
  }

}