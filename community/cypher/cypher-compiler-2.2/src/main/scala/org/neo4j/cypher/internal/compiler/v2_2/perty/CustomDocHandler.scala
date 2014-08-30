package org.neo4j.cypher.internal.compiler.v2_2.perty

import org.neo4j.cypher.internal.compiler.v2_2.perty.print.pprintToString

abstract class CustomDocHandler[T] extends DocHandler[T] {
  trait ToString[S <: T] {
    prettySelf: S with DocFormatting =>

    override def toString =
      pprintToString[S](prettySelf, formatter = prettySelf.docFormatter)(docGen.asConverter)
  }
}
