package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression

trait MatchPredicateNormalizer {
  val extract: PartialFunction[AnyRef, IndexedSeq[Expression]]
  val replace: PartialFunction[AnyRef, AnyRef]
}
