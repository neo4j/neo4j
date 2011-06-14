package org.neo4j.cypher.pipes

import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.ReturnItem

class ColumnFilterPipe(returnItems: Seq[ReturnItem], source: Pipe) extends Pipe {
  val symbols: SymbolTable = new SymbolTable(returnItems.map(x => x.identifier.name -> x.identifier).toMap)

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(row => {
      val filtered = row.filter((kv) => kv match { case (name,_) => returnItems.exists( _.identifier.name == name) } )
      f.apply(filtered)
    })
  }
}