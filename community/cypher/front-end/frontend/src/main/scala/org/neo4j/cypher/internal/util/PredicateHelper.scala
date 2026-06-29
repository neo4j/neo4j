/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.OperatorExpression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.TypeSignatures
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

object PredicateHelper {

  /**
   * Takes predicates and coerce them to a boolean operator and AND together the result
   * @param predicates The predicates to coerce
   * @return coerced predicates anded together
   */
  def coercePredicatesWithAnds(predicates: Iterable[Expression]): Option[Ands] = {
    Option.when(predicates.nonEmpty) {
      Ands(predicates.map(coerceToPredicate))(predicates.map(coerceToPredicate).head.position)
    }
  }

  def coercePredicates(predicates: ListSet[Expression]): Expression = Ands.create(predicates.map(coerceToPredicate))

  def coerceToPredicate(predicate: Expression): Expression = predicate match {
    case e: ListComprehension => GreaterThan(
        FunctionInvocation(FunctionName(functions.Size.name)(e.position), e)(e.position),
        SignedDecimalIntegerLiteral("0")(e.position.zeroLength)
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
        }
      case f: ResolvedFunctionInvocation => f.fcnSignature.forall(_.outputType == symbols.CTBoolean)
      case _                             => false
    }
  }
}
