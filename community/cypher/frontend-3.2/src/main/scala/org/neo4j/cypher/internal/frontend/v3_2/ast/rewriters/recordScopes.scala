package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{MapProjection, PatternComprehension}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, SemanticState, topDown}

case class recordScopes(semanticState: SemanticState) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case x: PatternComprehension =>
      x.withOuterScope(semanticState.recordedScopes(x).symbolDefinitions.map(_.asVariable))
    case x: MapProjection =>
      x.withOuterScope(semanticState.recordedScopes(x))
  })
}
