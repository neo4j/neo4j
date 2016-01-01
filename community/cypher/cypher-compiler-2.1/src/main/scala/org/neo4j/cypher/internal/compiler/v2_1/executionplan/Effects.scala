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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_1.commands.{ReturnItem, SortItem}
import org.neo4j.cypher.internal.compiler.v2_1.mutation.{Effectful, UpdateAction}
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

case class Effects(value: Int)  {
  def &(other: Effects): Effects = Effects(other.value & value)
  def |(other: Effects): Effects = Effects(other.value | value)

  def contains(other: Effects): Boolean = (value & other.value) == other.value
  def intersects(other: Effects): Boolean = (value & other.value) != 0
  def reads(): Boolean = intersects(Effects.READS_ENTITIES)
  def writes(): Boolean = intersects(Effects.WRITES_ENTITIES)

  override def toString =
    if (value == 0) "NONE"
    else {
      Seq("WRITES_NODES", "WRITES_RELATIONSHIPS", "READS_NODES", "READS_RELATIONSHIPS").zipWithIndex
        .filter { case (_: String, index: Int) => (value & (2 << index)) != 0}.map(_._1).mkString(" | ")
    }
}

object Effects {
  val WRITES_NODES = Effects(2 << 0)
  val WRITES_RELATIONSHIPS = Effects(2 << 1)
  val READS_NODES = Effects(2 << 2)
  val READS_RELATIONSHIPS = Effects(2 << 3)

  val NONE = Effects(0)
  val READS_ENTITIES = READS_NODES | READS_RELATIONSHIPS
  val WRITES_ENTITIES = WRITES_NODES | WRITES_RELATIONSHIPS
  val ALL = READS_ENTITIES | WRITES_ENTITIES

  implicit class TraversableEffects(iter: Traversable[Effectful]) {
    def effects: Effects = iter.map(_.effects).reduced
  }

  implicit class TraversableExpressions(iter: Traversable[Expression]) {
    def effects: Effects = iter.map(_.effects).reduced
  }

  implicit class EffectfulReturnItems(iter: Traversable[ReturnItem]) {
    def effects: Effects = iter.map(_.expression. effects).reduced
  }

  implicit class EffectfulUpdateAction(commands: Traversable[UpdateAction]) {
    def effects(symbols: SymbolTable): Effects = commands.map(_.effects(symbols)).reduced
  }

  implicit class MapEffects(m: Map[_, Expression]) {
    def effects: Effects = m.values.map(_.effects).reduced
  }

  implicit class SortItemEffects(m: Traversable[SortItem]) {
    def effects: Effects = m.map(_.expression.effects).reduced
  }

  implicit class ReducedEffects(effects: Traversable[Effects]) {
    def reduced = effects.reduceOption(_ | _).getOrElse(Effects.NONE)
  }
}
