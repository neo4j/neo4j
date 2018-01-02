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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.{AstNode, EffectfulAstNode}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.pipes
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.{SymbolTable, TypeSafe}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._


trait UpdateAction extends TypeSafe with AstNode[UpdateAction] {
  self =>

  def exec(context: ExecutionContext, state: pipes.QueryState): Iterator[ExecutionContext]

  def identifiers: Seq[(String, CypherType)]

  def rewrite(f: Expression => Expression): UpdateAction

  def shortName: String = getClass.getSimpleName.replace("Action", "")

  def arguments: Seq[Argument] = Seq(UpdateActionName(shortName))

  def effects(symbols: SymbolTable): Effects = {
    val collector = new EffectsCollector(localEffects(symbols), self, symbols)
    visitFirst {
      case (effectful: pipes.Effectful) => collector.register(effectful)
        .withEffects(effectful.localEffects.toWriteEffects)
      case (effectfulAst: EffectfulAstNode[_]) => collector.register(effectfulAst)
        .withEffects(effectfulAst.localEffects(updateSymbols(symbols)).toWriteEffects)
      case (update: UpdateAction) =>
        val oldSymbols = collector.symbols(update)
        collector
          .registerWithSymbolTable(update, update.updateSymbols(oldSymbols))
          .withEffects(update.localEffects(oldSymbols))
      case other => collector.register(other)
    }

    collector.effects
  }

  // This is here to give FOREACH action a chance to introduce symbols
  def updateSymbols(symbol: SymbolTable): SymbolTable = symbol.add(identifiers.toMap)

  def localEffects(symbols: SymbolTable): Effects
}

class EffectsCollector(private var _effects: Effects, expr: AstNode[_], symbolTable: SymbolTable) {
  self =>

  private val tables = new java.util.IdentityHashMap[AstNode[_], SymbolTable]()
  tables.put(expr, symbolTable)

  def withEffects(extraEffects: Effects) = {
    _effects = _effects | extraEffects
    self
  }

  def effects = _effects

  def register(expr: AstNode[_]): EffectsCollector = registerWithSymbolTable(expr, tables.get(expr))

  def registerWithSymbolTable(expr: AstNode[_], symbols: SymbolTable): EffectsCollector = {
    expr.children.foldLeft(symbols) {
      case (acc, elem) => elem match {
        case kid: UpdateAction =>
          val updated = kid.updateSymbols(acc)
          tables.put(kid, updated)
          updated
        case kid =>
          tables.put(kid, acc)
          acc
      }
    }
    self
  }
  def symbols(expr: AstNode[_]) = tables.get(expr)
}
