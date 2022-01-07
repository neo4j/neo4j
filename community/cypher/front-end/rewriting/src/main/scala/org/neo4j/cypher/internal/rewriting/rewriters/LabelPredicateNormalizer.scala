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
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or

object LabelPredicateNormalizer extends MatchPredicateNormalizer {
  override val extract: PartialFunction[AnyRef, IndexedSeq[Expression]] = {
    case p@NodePattern(Some(id), labels, _, _, _) if labels.nonEmpty => Vector(HasLabels(id.copyId, labels)(p.position))
    case NodePattern(Some(id), _, Some(expression), _, _)            => Vector(extractLabelExpressionPredicates(id, expression))
  }

  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p@NodePattern(Some(_), labels, _, _, _) if labels.nonEmpty => p.copy(labels = Seq.empty)(p.position)
    case p@NodePattern(Some(_), _, Some(_), _, _)                   => p.copy(labelExpression = None)(p.position)
  }

  private def extractLabelExpressionPredicates(variable: LogicalVariable, e: LabelExpression): BooleanExpression = {
    e match {
      case c: LabelExpression.Conjunction => And(
        extractLabelExpressionPredicates(variable.copyId, c.lhs),
        extractLabelExpressionPredicates(variable.copyId, c.rhs)
      )(c.position)

      case d: LabelExpression.Disjunction => Or(
        extractLabelExpressionPredicates(variable.copyId, d.lhs),
        extractLabelExpressionPredicates(variable.copyId, d.rhs)
      )(d.position)

      case n: LabelExpression.Negation => Not(
        extractLabelExpressionPredicates(variable.copyId, n.e)
      )(n.position)

      case n: LabelExpression.Label => HasLabels(variable.copyId, Seq(LabelName(n.label.name)(n.position)))(n.position)
    }
  }
}
