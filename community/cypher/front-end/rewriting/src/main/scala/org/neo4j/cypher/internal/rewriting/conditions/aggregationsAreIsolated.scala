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
package org.neo4j.cypher.internal.rewriting.conditions

import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren

case object aggregationsAreIsolated extends ValidatingCondition {

  override def apply(that: Any)(cancellationChecker: CancellationChecker): Seq[String] = {
    that.folder(cancellationChecker).treeFold(Seq.empty[String]) {
      case expr: Expression if hasAggregateButIsNotAggregate(expr)(cancellationChecker) =>
        acc => SkipChildren(acc :+ s"Expression $expr contains child expressions which are aggregations")
    }
  }

  override def name: String = productPrefix
}

object hasAggregateButIsNotAggregate {

  def apply(expression: Expression)(cancellationChecker: CancellationChecker): Boolean = expression match {
    case IsAggregate(_) => false
    case _: FullSubqueryExpression =>
      false // Full Subquery Expressions contain Regular Queries which can have aggregations
    case e: Expression =>
      e.folder(cancellationChecker).treeFold[Boolean](false) {
        case _: FullSubqueryExpression => SkipChildren(_)
        case IsAggregate(_)            => _ => SkipChildren(true)
      }
  }
}
