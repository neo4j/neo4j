package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.Rewriter
import org.neo4j.cypher.internal.compiler.v2_1.ast.{FunctionInvocation, Identifier, Equals}

object normalizeEqualsArgumentOrder extends Rewriter {
  override def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val instance: Rewriter = Rewriter.lift {
    // moved identifiers on equals to the left
    case predicate @ Equals(Identifier(_), _) =>
      predicate
    case predicate @ Equals(lhs, rhs @ Identifier(_)) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)

    // move id(n) on equals to the left
    case predicate @ Equals(FunctionInvocation(Identifier("id"), _, _), _) =>
      predicate
    case predicate @ Equals(lhs, rhs @ FunctionInvocation(Identifier("id"), _, _)) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)
  }
}
