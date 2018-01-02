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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, ReadsRelationships}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CastSupport
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CastSupport.castOrFail
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Relationship
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

case class RelationshipEndPoints(relExpression: Expression, start: Boolean) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = relExpression(ctx) match {
    case null => null
    case value =>
      val rel = castOrFail[Relationship](value)

      if (start)
        state.query.relationshipStartNode(rel)
      else
        state.query.relationshipEndNode(rel)
  }

  def arguments = Seq(relExpression)

  protected def calculateType(symbols: SymbolTable): CypherType = CTNode

  def rewrite(f: (Expression) => Expression): Expression = f(RelationshipEndPoints(relExpression.rewrite(f), start))

  def symbolTableDependencies: Set[String] = relExpression.symbolTableDependencies

  override def localEffects(symbols: SymbolTable) = Effects(ReadsRelationships)
}
