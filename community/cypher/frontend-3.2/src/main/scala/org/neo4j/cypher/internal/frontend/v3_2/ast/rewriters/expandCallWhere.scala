package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}

// Rewrites CALL proc WHERE <p> ==> CALL proc WITH * WHERE <p>
case object expandCallWhere extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case query@SingleQuery(clauses) =>
      val newClauses = clauses.flatMap {
        case unresolved@UnresolvedCall(_, _, _, Some(result@ProcedureResult(_, optWhere@Some(where)))) =>
          val newResult = result.copy(where = None)(result.position)
          val newUnresolved = unresolved.copy(declaredResult = Some(newResult))(unresolved.position)
          val newItems = ReturnItems(includeExisting = true, Seq.empty)(where.position)
          val newWith = With(distinct = false, newItems, None, None, None, optWhere)(where.position)
          Seq(newUnresolved, newWith)

        case clause =>
          Some(clause)
      }
      query.copy(clauses = newClauses)(query.position)
  })

  override def apply(v: AnyRef): AnyRef =
    instance(v)
}
