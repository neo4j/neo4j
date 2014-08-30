package org.neo4j.cypher.internal.compiler.v2_2.perty

import org.neo4j.cypher.internal.compiler.v2_2.perty.bling.SingleFunDigger
import org.neo4j.cypher.internal.compiler.v2_2.perty.print.pprintToString

import scala.reflect.ClassTag

abstract class CustomDocGen[-T : ClassTag] extends SingleFunDigger[T, Any, Doc] {
  object drill extends DocDrill[T] {
    private val impl: DocDrill[T] = newDocDrill

    def apply(v: T) = impl(v)
  }

  protected def newDocDrill: DocDrill[T]

  trait ToString[S <: T] {
    prettySelf: S with DocFormatting =>

    override def toString =
      pprintToString[S](prettySelf, formatter = docFormatter)(asConverter)
  }
}
