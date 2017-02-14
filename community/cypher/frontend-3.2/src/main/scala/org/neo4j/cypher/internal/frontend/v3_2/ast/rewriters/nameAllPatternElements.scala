package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{NodePattern, RelationshipPattern, Variable}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object nameAllPatternElements extends Rewriter {

  override def apply(in: AnyRef): AnyRef = namingRewriter.apply(in)

  val namingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case pattern: NodePattern if pattern.variable.isEmpty =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if pattern.variable.isEmpty  =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)
  })
}
