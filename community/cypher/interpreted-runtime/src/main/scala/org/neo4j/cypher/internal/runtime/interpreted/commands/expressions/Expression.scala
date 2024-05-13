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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.values.AnyValue

abstract class Expression extends AstNode[Expression] {

  def rewrite(f: Expression => Expression): Expression

  def rewriteAsPredicate(f: Expression => Expression): Predicate = rewrite(f) match {
    case pred: Predicate => pred
    case e               => CoercedPredicate(e)
  }

  // Expressions that do not get anything in their context from this expression.
  def arguments: collection.Seq[Expression]

  // Any expressions that this expression builds on
  def children: collection.Seq[AstNode[_]]

  def containsAggregate: Boolean = exists(_.isInstanceOf[AggregationExpression])

  def apply(row: ReadableRow, state: QueryState): AnyValue

  override def toString: String = this match {
    case p: Product => scala.runtime.ScalaRunTime._toString(p)
    case _          => getClass.getSimpleName
  }

  val isDeterministic: Boolean = !exists {
    case RandFunction()       => true
    case RandomUUIDFunction() => true
    case _                    => false
  }
}

case class CachedExpression(key: String, typ: CypherType) extends Expression {
  def apply(row: ReadableRow, state: QueryState): AnyValue = row.getByName(key)

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq.empty

  override def children: Seq[AstNode[_]] = Seq.empty

  override def toString: String = "Cached(%s of type %s)".format(key, typ)
}

abstract class Arithmetics(left: Expression, right: Expression) extends Expression {
  override def arguments: Seq[Expression] = Seq(left, right)

  override def children: Seq[AstNode[_]] = Seq(left, right)
}

trait ExtendedExpression extends Expression {
  def legacy: Expression
}
