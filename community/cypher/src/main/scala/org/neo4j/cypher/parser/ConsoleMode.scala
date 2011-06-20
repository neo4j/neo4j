package org.neo4j.cypher.parser

import org.neo4j.cypher.commands._

trait ConsoleMode extends ReturnItems {
  override  def propertyOutput:Parser[ReturnItem] = identity ~ "." ~ identity ^^ { case c ~ "." ~ p => NullablePropertyOutput(c,p) }
}

class ConsoleCypherParser extends CypherParser with ConsoleMode {

}