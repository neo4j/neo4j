/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.predicates

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList

case class Ors(predicates: NonEmptyList[Predicate]) extends CompositeBooleanPredicate {
  def shouldExitWhen = true
  def rewrite(f: (Expression) => Expression): Expression = f(Ors(predicates.map(_.rewriteAsPredicate(f))))
}

@deprecated("Use Ors (plural) instead")
case class Or(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = Ors(NonEmptyList(a, b)).isMatch(m)

  override def toString: String = s"($a OR $b)"
  def containsIsNull = a.containsIsNull || b.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(Or(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  def arguments = Seq(a, b)

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
