package org.neo4j.cypher.internal.ir.v3_1

import org.neo4j.cypher.internal.frontend.v3_1.ast.Expression

case class Predicate(dependencies: Set[IdName], expr: Expression) {

  def hasDependenciesMet(symbols: Set[IdName]): Boolean =
    (dependencies -- symbols).isEmpty

  def hasDependenciesMetForRequiredSymbol(symbols: Set[IdName], required: IdName): Boolean =
    dependencies.contains(required) && hasDependenciesMet(symbols)
}

object Predicate {
  implicit val byPosition = Ordering.by { (predicate: Predicate) => predicate.expr.position }
}