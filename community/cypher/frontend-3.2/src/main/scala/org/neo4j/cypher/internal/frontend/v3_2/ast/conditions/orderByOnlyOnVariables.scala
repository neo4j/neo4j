package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.ast.{OrderBy, SortItem, Variable}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object orderByOnlyOnVariables extends Condition {
  def apply(that: Any): Seq[String] = {
    val orderBys = collectNodesOfType[OrderBy].apply(that)
    orderBys.flatMap { orderBy =>
      orderBy.sortItems.collect {
        case item: SortItem if !item.expression.isInstanceOf[Variable] =>
          s"OrderBy at ${orderBy.position} is ordering on an expression (${item.expression}) instead of a variable"
      }
    }
  }

  override def name: String = productPrefix
}
