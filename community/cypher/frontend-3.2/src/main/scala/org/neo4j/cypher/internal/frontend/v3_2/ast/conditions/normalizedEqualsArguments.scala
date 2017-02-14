package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object normalizedEqualsArguments extends Condition {
  def apply(that: Any): Seq[String] = {
    val equals = collectNodesOfType[Equals].apply(that)
    equals.collect {
      case eq@Equals(expr, Property(_,_)) if !expr.isInstanceOf[Property] && notIdFunction(expr) =>
        s"Equals at ${eq.position} is not normalized: $eq"
      case eq@Equals(expr, func@FunctionInvocation(_, _, _, _)) if isIdFunction(func) && notIdFunction(expr) =>
        s"Equals at ${eq.position} is not normalized: $eq"
    }
  }

  private def isIdFunction(func: FunctionInvocation) = func.function == functions.Id

  private def notIdFunction(expr: Expression) =
    !expr.isInstanceOf[FunctionInvocation] || !isIdFunction(expr.asInstanceOf[FunctionInvocation])

  override def name: String = productPrefix
}
