package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, Match}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object nameMatchPatternElements extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case m: Match =>
      val rewrittenPattern = m.pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      m.copy(pattern = rewrittenPattern)(m.position)
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])
}
