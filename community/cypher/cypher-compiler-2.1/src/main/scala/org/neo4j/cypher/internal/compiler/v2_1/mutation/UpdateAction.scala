/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.mutation

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.commands.{AstNode, EffectfulAstNode}
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._

trait WritesNodes
trait WritesRelationships

trait Effectful {
  def effects: Effects
  def localEffects: Effects
}

trait UpdateAction extends TypeSafe with AstNode[UpdateAction] {
  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext]

  def identifiers: Seq[(String, CypherType)]

  def rewrite(f: Expression => Expression): UpdateAction

  def shortName: String = getClass.getSimpleName.replace("Action", "")

  def arguments: Seq[Argument] = identifiers.map(tuple => IntroducedIdentifier(tuple._1)) :+ UpdateActionName(shortName)

  def effects(symbols: SymbolTable): Effects = {
    var completeEffects = localEffects(symbols)
    visitChildren {
      case (expr: Effectful)           => completeEffects = completeEffects | expr.localEffects
      case (expr: EffectfulAstNode[_]) => completeEffects = completeEffects | expr.localEffects
      case (expr: UpdateAction)        => completeEffects = completeEffects | expr.localEffects(updateSymbols(symbols))
    }
    completeEffects
  }

  // This is here to give FOREACH action a chance to introduce symbols
  def updateSymbols(symbol: SymbolTable ): SymbolTable = symbol

  def localEffects(symbols: SymbolTable): Effects
}
