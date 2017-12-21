package org.neo4j.cypher.internal.util.v3_4.attribution

import org.neo4j.cypher.internal.util.v3_4.Unchangeable

import scala.collection.mutable.ArrayBuffer

trait Attribute[T] {

  // TODO crappy perf?
  val array:ArrayBuffer[Unchangeable[T]] = new ArrayBuffer[Unchangeable[T]]()

  def set(id:Id, t:T): Unit = {
    val requiredSize = id.x + 1
    if (array.size < requiredSize) {
      while (array.size < requiredSize)
        array += new Unchangeable
      array(id.x).value = t
    } else {
      val prev = array(id.x)
      array(id.x).value = t
    }
  }

  def get(id:Id): T = {
    array(id.x).value
  }

  def copy(from:Id, to:Id): Unit = {
    set(to, get(from))
  }

  override def toString(): String = {
    val sb = new StringBuilder
    sb ++= this.getClass.getSimpleName + "\n"
    for ( i <- array.indices )
      sb ++= s"$i : ${array(i)}\n"
    sb.result()
  }
}