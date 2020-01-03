/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.logical.plans.{CoerceToPredicate, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.exceptions.InternalException

object PredicateHelper {

  private val BOOLEAN_FUNCTIONS: Set[functions.Function] = Set(functions.Exists, functions.ToBoolean)
  /**
    * Takes predicates and coerce them to a boolean operator and AND together the result
    * @param predicates The predicates to coerce
    * @return coerced predicates anded together
    */
  def coercePredicatesWithAnds(predicates: Seq[Expression]): Ands = {
    if (predicates.isEmpty) throw new InternalException("Selection need at least one predicate")
    Ands(predicates.map(coerceToPredicate).toSet)(predicates.map(coerceToPredicate).head.position)
  }

  def coercePredicates(predicates: Seq[Expression]): Expression = Ands.create(predicates.map(coerceToPredicate).toSet)

  def coerceToPredicate(predicate: Expression): Expression = predicate match {
    case e: PatternExpression =>
      GreaterThan(
        FunctionInvocation(FunctionName(functions.Length.name)(e.position), e)(e.position),
        UnsignedDecimalIntegerLiteral("0")(e.position))(e.position)
    case e: ListComprehension => GreaterThan(
      FunctionInvocation(FunctionName(functions.Size.name)(e.position), e)(e.position),
      UnsignedDecimalIntegerLiteral("0")(e.position))(e.position)
    case e if isPredicate(e) => e
    case e => CoerceToPredicate(e)
  }

  //TODO we should be able to use the semantic table for this however for two reasons we cannot
  //i) we do late ast rewrite after semantic analysis, so all semantic table will be missing some expression
  //ii) For WHERE a.prop semantic analysis will say that a.prop has boolean type since it belongs to a WHERE.
  //    That makes it not usable here since we would need to coerce in that case.
  def isPredicate(expression: Expression): Boolean = {
    expression match {
      case o: OperatorExpression => o.signatures.forall(_.outputType == symbols.CTBoolean)
      case f: FunctionInvocation => BOOLEAN_FUNCTIONS.contains(f.function)
      case f: ResolvedFunctionInvocation => f.fcnSignature.forall(_.outputType == symbols.CTBoolean)
      case  _:Ands | _: Ors | _: In | _:BooleanLiteral | _:HasLabels | _:AndedPropertyInequalities |  _:IterablePredicateExpression => true
      case _:ExistsSubClause => true
      case _ => false
    }
  }
}
