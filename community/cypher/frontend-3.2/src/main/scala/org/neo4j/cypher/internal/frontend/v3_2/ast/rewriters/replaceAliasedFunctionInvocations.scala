package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{FunctionInvocation, FunctionName}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

import scala.collection.immutable.TreeMap

case object replaceAliasedFunctionInvocations extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  /*
   * These are historical names for functions. They are all subject to removal in an upcoming major release.
   */
  val aliases: Map[String, String] = TreeMap("toInt" -> "toInteger",
                                             "upper" -> "toUpper",
                                             "lower" -> "toLower",
                                             "rels" -> "relationships")(CaseInsensitiveOrdered)

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case func@FunctionInvocation(_, f@FunctionName(name), _, _) if aliases.get(name).nonEmpty =>
      func.copy(functionName = FunctionName(aliases(name))(f.position))(func.position)
  })

}

object CaseInsensitiveOrdered extends Ordering[String] {
  def compare(x: String, y: String): Int =
    x.compareToIgnoreCase(y)
}
