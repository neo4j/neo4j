/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

case class LabelExpressionNormalizer(entityExpression: Expression, entityType: Option[EntityType]) extends Rewriter {
  val instance: Rewriter = Rewriter.lift {
    case c: LabelExpression.Conjunction => And(
      c.lhs,
      c.rhs
    )(c.position)
    case c: LabelExpression.ColonConjunction => And(
      c.lhs,
      c.rhs
    )(c.position)

    case d: LabelExpression.Disjunction => Or(
      d.lhs,
      d.rhs
    )(d.position)

    case n: LabelExpression.Negation => Not(
      n.e
    )(n.position)

    case n: LabelExpression.Wildcard =>
      val size: Expression => FunctionInvocation = FunctionInvocation(FunctionName("size")(n.position), _)(n.position)
      val labels: Expression => FunctionInvocation = FunctionInvocation(FunctionName("labels")(n.position), _)(n.position)
      val zero = SignedDecimalIntegerLiteral("0")(n.position)

      GreaterThan(size(labels(copy(entityExpression))), zero)(n.position)

    case n: LabelExpression.Label =>
      entityType match {
        case Some(NODE_TYPE)         => HasLabels(copy(entityExpression), Seq(LabelName(n.label.name)(n.position)))(n.position)
        case Some(RELATIONSHIP_TYPE) => HasTypes(copy(entityExpression), Seq(RelTypeName(n.label.name)(n.position)))(n.position)
        case None                    => HasLabelsOrTypes(copy(entityExpression), Seq(LabelOrRelTypeName(n.label.name)(n.position)))(n.position)
      }
  }

  /**
   * Needed to fulfil `noReferenceEqualityAmongVariables`
   */
  def copy(expr: Expression): Expression = expr match {
    case variable: LogicalVariable => variable.copyId
    case _ => expr
  }

  override def apply(v1: AnyRef): AnyRef = topDown(instance)(v1)
}
