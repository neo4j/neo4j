package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Equals, Not, NotEquals}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, topDown}

case object normalizeNotEquals extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case p @ NotEquals(lhs, rhs) =>
      Not(Equals(lhs, rhs)(p.position))(p.position)   // not(1 = 2)  <!===!>     1 != 2
  })
}
