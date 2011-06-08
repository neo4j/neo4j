package org.neo4j.cypher.pipes

import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.{True, Clause}
import java.lang.String

/**
 * @author mh
 * @since 09.06.11
 */

class FilterPipe(val where: Option[Clause], source: Pipe) extends Pipe {

  def createFilters(where: Option[Clause], symbolTable: SymbolTable): Clause = {
    where match {
      case None => new True()
      case Some(clause) => clause
    }
  }

  def columnNames = null
  var filters : Clause = null

  def prepare(symbolTable: SymbolTable) {
    filters = createFilters(where,symbolTable)
  }

  def foreach[U](f: (Map[String, Any]) => U) {
    source.filter((row) => {
      filters.isMatch(row)
    }).foreach(f)
  }
}