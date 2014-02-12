package org.neo4j.cypher.internal.compiler.v2_0.helpers

object StringHelpers {
  implicit class RichString(val text: String) extends AnyVal {
    def stripLinesAndMargins: String =
      text.stripMargin.filter( (ch: Char) => Character.isDefined(ch) && !Character.isISOControl(ch) )
  }
}
