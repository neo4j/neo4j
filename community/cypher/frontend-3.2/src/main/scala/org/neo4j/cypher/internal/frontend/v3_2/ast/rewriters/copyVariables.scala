package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.Variable
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object copyVariables extends Rewriter {
  private val instance = bottomUp(Rewriter.lift { case variable: Variable => variable.copyId })

  def apply(that: AnyRef): AnyRef = instance.apply(that)
}
