package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.Foldable.FoldableAny
import org.neo4j.cypher.internal.frontend.v3_2.ast.ASTNode
import org.neo4j.cypher.internal.frontend.v3_2.{AstRewritingMonitor, Rewriter}

import scala.annotation.tailrec
/*
This rewriter tries to limit rewriters that grow the product AST too much
 */
case class repeatWithSizeLimit(rewriter: Rewriter)(implicit val monitor: AstRewritingMonitor) extends Rewriter {

  private def astNodeSize(value: Any): Int = value.treeFold(1) {
    case _: ASTNode => acc => (acc + 1, Some(identity))
  }

  final def apply(that: AnyRef): AnyRef = {
    val initialSize = astNodeSize(that)
    val limit = initialSize * initialSize

    innerApply(that, limit)
  }

  @tailrec
  private def innerApply(that: AnyRef, limit: Int): AnyRef = {
    val t = rewriter.apply(that)
    val newSize = astNodeSize(t)

    if (newSize > limit) {
      monitor.abortedRewriting(that)
      that
    }
    else if (t == that) {
      t
    }
    else {
      innerApply(t, limit)
    }
  }
}
