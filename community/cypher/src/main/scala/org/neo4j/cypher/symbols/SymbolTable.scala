package org.neo4j.cypher.symbols

import org.neo4j.cypher.SyntaxException

class SymbolTable(identifiers:Identifier*) {

  assertNoClashesExist()

  def assertHas(expected:Identifier) {
    identifiers.find(_.name == expected.name) match {
      case None => throw new SyntaxException("No identifier named " + expected.name + " found.")
      case Some(existing) => if (!expected.typ.isAssignableFrom(existing.typ)) {
        throw new SyntaxException("Expected `" + expected.name + "` to be a " + expected.typ + " but it was " + existing.typ)
      }
    }
  }

  def add(newIdentifiers:Identifier*):SymbolTable = new SymbolTable(  (identifiers ++ newIdentifiers):_* )

  private def assertNoClashesExist() {
    val names: Set[String] = identifiers.map(_.name).toSet

    if(names.size != identifiers.size) {
      names.foreach( n=> if(identifiers.filter(_.name == n).size > 1) throw new SyntaxException("Identifier " + n + " defined multiple times"))
    }
  }
}