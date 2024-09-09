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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.CaseExpression.Placeholder
import org.neo4j.cypher.internal.util.InputPosition

case class CaseExpression(
  candidate: Option[Expression],
  candidateVarName: Option[LogicalVariable],
  alternatives: IndexedSeq[(Expression, Expression)],
  default: Option[Expression]
)(val position: InputPosition) extends ScopeExpression {

  lazy val possibleExpressions: IndexedSeq[Expression] = alternatives.map(_._2) ++ default

  override def introducedVariables: Set[LogicalVariable] = candidateVarName.toSet

  override def scopeDependencies: Set[LogicalVariable] = {
    val deps = candidate.fold(Set.empty[LogicalVariable])(_.dependencies) ++
      alternatives.flatMap(p => p._1.dependencies ++ p._2.dependencies) ++
      default.fold(Set.empty[LogicalVariable])(_.dependencies)
    deps -- introducedVariables
  }

  override def isConstantForQuery: Boolean =
    candidate.forall(_.isConstantForQuery) &&
      alternatives.forall(t => t._1.isConstantForQuery && t._2.isConstantForQuery) &&
      default.forall(_.isConstantForQuery)

  def withDefault(default: Expression): CaseExpression =
    copy(default = Some(default))(position)

  def withCase(predicate: Expression, result: Expression): CaseExpression =
    copy(alternatives = alternatives :+ (predicate, result))(position)

  def withExtendedCase(predicate: Expression => Expression, result: Expression): CaseExpression =
    copy(alternatives = alternatives :+ (predicate(Placeholder), result))(position)

  def withCandidate(candidate: Expression): CaseExpression =
    copy(candidate = Some(candidate))(position)
}

object CaseExpression {

  def apply(
    candidate: Option[Expression],
    alternatives: List[(Expression, Expression)],
    default: Option[Expression]
  )(position: InputPosition): CaseExpression =
    CaseExpression(candidate, None, alternatives.toIndexedSeq, default)(position)

  /**
   * Represents the candidate expression when used as LHS of a predicate in an 'extended' Case expression
   */
  case class Placeholder(position: InputPosition) extends Expression {
    def isConstantForQuery: Boolean = false
  }

  object Placeholder extends Placeholder(InputPosition.NONE) {
    override def dup(children: Seq[AnyRef]): Placeholder.this.type = this
  }

  val empty: CaseExpression = CaseExpression(None, None, IndexedSeq.empty, None)(InputPosition.NONE)
}
