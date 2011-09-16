package org.neo4j.cypher.pipes

import org.neo4j.cypher.SymbolTable
import org.neo4j.cypher.commands.{Identifier, LiteralIdentifier}

class ParameterPipe(params: Map[String, Any]) extends Pipe {
  def foreach[U](f: (Map[String, Any]) => U) {
    f(params)
  }

  val identifiers: Seq[Identifier] = params.keys.map(k => LiteralIdentifier("$" + k)).toSeq

  val symbols: SymbolTable = new SymbolTable(identifiers)
}