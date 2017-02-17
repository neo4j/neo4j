/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterCondition
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseState, Condition}

case class StatementCondition(inner: Any => Seq[String]) extends Condition {
  override def check(state: AnyRef): Seq[String] = state match {
    case s: BaseState => inner(s.statement())
    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }
}

object StatementCondition {
  def apply(inner: RewriterCondition) = new StatementCondition(inner.condition)
}
