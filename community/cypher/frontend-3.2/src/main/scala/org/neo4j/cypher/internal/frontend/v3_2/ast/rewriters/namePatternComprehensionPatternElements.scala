package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.PatternComprehension
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object namePatternComprehensionPatternElements extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case expr: PatternComprehension =>
      val (namedComprehension, _) = PatternExpressionPatternElementNamer(expr)
      namedComprehension
  })

  override def apply(v: AnyRef): AnyRef = instance(v)
}
