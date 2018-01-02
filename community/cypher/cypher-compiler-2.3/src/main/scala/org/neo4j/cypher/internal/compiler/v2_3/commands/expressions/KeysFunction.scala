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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Relationship}

case class KeysFunction(expr: Expression) extends NullInNullOutExpression(expr) {

  override def compute(value: Any, ctx: ExecutionContext)(implicit state: QueryState) = value match {
    case IsMap(map) => map(state.query).keys.toList

    case x =>
      throw new CypherTypeException(s"""Expected ${expr} to be a node, relationship,or a literal map,
           |but it was ${x.getClass.getSimpleName}""".stripMargin)
  }

  def rewrite(f: (Expression) => Expression) = f(KeysFunction(expr.rewrite(f)))

  def arguments = Seq(expr)

  def symbolTableDependencies = expr.symbolTableDependencies

  protected def calculateType(symbols: SymbolTable) = expr match {
    case node: Node => expr.evaluateType(CTNode, symbols)
    case rel: Relationship => expr.evaluateType(CTRelationship, symbols)
    case _ => CTCollection(CTString)
  }

  override def localEffects(symbols: SymbolTable) = expr match {
    case i: Identifier => symbols.identifiers(i.entityName) match {
      case _: NodeType => Effects(ReadsAnyNodeProperty)
      case _: RelationshipType => Effects(ReadsAnyRelationshipProperty)
      case _ => Effects()
    }
    case _ => Effects()
  }
}
