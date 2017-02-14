package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.Foldable.FoldableAny
import org.neo4j.cypher.internal.frontend.v3_2.ast.{PatternComprehension, PatternElement, RelationshipsPattern}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.Condition

case object noUnnamedPatternElementsInPatternComprehension extends Condition {

  override def name: String = productPrefix

  override def apply(that: Any): Seq[String] = that.treeFold(Seq.empty[String]) {
    case expr: PatternComprehension if containsUnNamedPatternElement(expr.pattern) =>
      acc => (acc :+ s"Expression $expr contains pattern elements which are not named", None)
  }

  private def containsUnNamedPatternElement(expr: RelationshipsPattern) = expr.exists {
    case p: PatternElement => p.variable.isEmpty
  }
}
