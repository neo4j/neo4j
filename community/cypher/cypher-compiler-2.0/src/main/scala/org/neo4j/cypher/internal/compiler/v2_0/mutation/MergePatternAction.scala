/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{SymbolTable, CypherType}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_0.commands.{Pattern, AstNode}

case class MergePatternAction(patterns: Seq[Pattern], actions: Seq[UpdateAction], onMatch: Seq[UpdateAction]) extends UpdateAction {
  def children: Seq[AstNode[_]] = ???

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = ???

  def identifiers: Seq[(String, CypherType)] = patterns.flatMap(_.possibleStartPoints)

  def rewrite(f: (Expression) => Expression): UpdateAction = MergePatternAction(patterns = patterns.map(_.rewrite(f)),
    actions = actions.map(_.rewrite(f)),
    onMatch = onMatch.map(_.rewrite(f)))

  def symbolTableDependencies: Set[String] = {
    val dependencies = (patterns.flatMap(_.symbolTableDependencies) ++ actions.flatMap(_.symbolTableDependencies)).toSet
    val introducedIdentifiers = actions.flatMap(_.identifiers.map(_._1))
    dependencies -- introducedIdentifiers
  }

  override def symbolDependenciesMet(symbols: SymbolTable): Boolean = {
    patterns.exists {
      pattern => pattern.possibleStartPoints.exists { case (k, _) => symbols.hasIdentifierNamed(k) }
    }
  }
}
