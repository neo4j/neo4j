/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.OperatorExpression
import org.neo4j.cypher.internal.expressions.TypeSignatures
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols

object PredicateHelper {

  /**
   * Takes predicates and coerce them to a boolean operator and AND together the result
   * @param predicates The predicates to coerce
   * @return coerced predicates anded together
   */
  def coercePredicatesWithAnds(predicates: Seq[Expression]): Option[Ands] = {
    Option.when(predicates.nonEmpty) {
      Ands(predicates.map(coerceToPredicate))(predicates.map(coerceToPredicate).head.position)
    }
  }

  def coercePredicates(predicates: ListSet[Expression]): Expression = Ands.create(predicates.map(coerceToPredicate))

  def coerceToPredicate(predicate: Expression): Expression = predicate match {
    case e: ListComprehension => GreaterThan(
        FunctionInvocation(FunctionName(functions.Size.name)(e.position), e)(e.position),
        UnsignedDecimalIntegerLiteral("0")(e.position)
      )(e.position)
    case e if isPredicate(e) => e
    case e                   => CoerceToPredicate(e)
  }

  // TODO we should be able to use the semantic table for this however for two reasons we cannot
  // i) we do late ast rewrite after semantic analysis, so all semantic table will be missing some expression
  // ii) For WHERE a.prop semantic analysis will say that a.prop has boolean type since it belongs to a WHERE.
  //    That makes it not usable here since we would need to coerce in that case.
  def isPredicate(expression: Expression): Boolean = {
    expression match {
      case _: BooleanExpression | _: BooleanLiteral => true
      case o: OperatorExpression                    => o.signatures.forall(_.outputType == symbols.CTBoolean)
      case f: FunctionInvocation => f.function match {
          case ts: TypeSignatures => ts.signatures.forall(_.outputType == symbols.CTBoolean)
          case func               => false
        }
      case f: ResolvedFunctionInvocation => f.fcnSignature.forall(_.outputType == symbols.CTBoolean)
      case _                             => false
    }
  }
}
