package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterCondition
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseState, Condition}

case class StatementCondition(inner: Any => Seq[String]) extends Condition {
  override def check(state: AnyRef): Seq[String] = state match {
    case s: BaseState => inner(s.statement())
    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }
}

object StatementCondition {
  def apply(inner: RewriterCondition) = new StatementCondition(inner.condition)
}
