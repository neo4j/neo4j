package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContext, Condition}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

case object rewriteEqualityToInPredicate extends StatementRewriter {

  override def description: String = "normalize equality predicates into IN comparisons"

  override def instance(ignored: BaseContext): Rewriter = bottomUp(Rewriter.lift {
    // id(a) = value => id(a) IN [value]
    case predicate@Equals(func@FunctionInvocation(_, _, _, IndexedSeq(idExpr)), idValueExpr)
      if func.function == functions.Id =>
      In(func, ListLiteral(Seq(idValueExpr))(idValueExpr.position))(predicate.position)

    // Equality between two property lookups should not be rewritten
    case predicate@Equals(_:Property, _:Property) =>
      predicate

    // a.prop = value => a.prop IN [value]
    case predicate@Equals(prop@Property(id: Variable, propKeyName), idValueExpr) =>
      In(prop, ListLiteral(Seq(idValueExpr))(idValueExpr.position))(predicate.position)
  })

  override def postConditions: Set[Condition] = Set.empty
}
