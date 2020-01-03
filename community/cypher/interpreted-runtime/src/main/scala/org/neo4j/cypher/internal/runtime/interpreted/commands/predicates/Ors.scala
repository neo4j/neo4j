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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.v3_5.util.NonEmptyList
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class Ors(predicates: NonEmptyList[Predicate]) extends CompositeBooleanPredicate {
  override def shouldExitWhen = true
  override def rewrite(f: Expression => Expression): Expression = f(Ors(predicates.map(_.rewriteAsPredicate(f))))
  override def children: Seq[AstNode[_]] = predicates.toIndexedSeq
}

@deprecated("Use Ors (plural) instead")
case class Or(a: Predicate, b: Predicate) extends Predicate {
  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = Ors(NonEmptyList(a, b)).isMatch(m, state)

  override def toString: String = s"($a OR $b)"
  override def containsIsNull: Boolean = a.containsIsNull || b.containsIsNull
  override def rewrite(f: Expression => Expression): Expression = f(Or(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  override def arguments: Seq[Expression] = Seq(a, b)

  override def children: Seq[AstNode[_]] = Seq(a, b)

  override def symbolTableDependencies: Set[String] = a.symbolTableDependencies ++ b.symbolTableDependencies
}
