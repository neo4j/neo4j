package org.neo4j.cypher.pipes

import org.neo4j.cypher.{Comparer, SymbolTable}

class SortPipe(sortIdentifier: String, inner: Pipe) extends Pipe with Comparer {
  val symbols: SymbolTable = inner.symbols

  def foreach[U](f: (Map[String, Any]) => U) {

    val sorted = inner.toList.sortWith((a, b) => {
      val aVal = a(sortIdentifier).asInstanceOf[String]
      val bVal = b(sortIdentifier).asInstanceOf[String]

      compare(aVal, bVal) < 1
    })

    sorted.foreach(f)
  }
}
