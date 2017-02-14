package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.Foldable.FoldableAny
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, hasAggregateButIsNotAggregate}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object aggregationsAreIsolated extends Condition {

  def apply(that: Any): Seq[String] = that.treeFold(Seq.empty[String]) {
    case expr: Expression if hasAggregateButIsNotAggregate(expr) =>
      acc => (acc :+ s"Expression $expr contains child expressions which are aggregations", None)
  }

  override def name: String = productPrefix
}
