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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ReturnItem, SortItem}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.UpdateAction
import org.neo4j.cypher.internal.compiler.v2_3.pipes.Effectful
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class Effects(effectsSet: Set[Effect] = Set.empty) {

  def &(other: Effects): Effects = Effects(other.effectsSet.intersect(effectsSet))

  def |(other: Effects): Effects = Effects(effectsSet ++ other.effectsSet)

  def contains(effect: Effect): Boolean = effectsSet(effect)

  def reads() = effectsSet.exists(_.reads)

  def writes() = effectsSet.exists(_.writes)

  def toWriteEffects = Effects(effectsSet.map {
    case e: ReadEffect => e.toWriteEffect
    case e: WriteEffect => e
  }.toSet[Effect])
}

object AllWriteEffects extends Effects(Set(WritesAnyNode, WritesRelationships, WritesAnyNodeProperty, WritesAnyRelationshipProperty)) {
  override def toString = "AllWriteEffects"
}

object AllReadEffects extends Effects(Set(ReadsAllNodes, ReadsRelationships, ReadsAnyNodeProperty, ReadsAnyRelationshipProperty)) {
  override def toString = "AllReadEffects"
}

object AllEffects extends Effects((AllWriteEffects | AllReadEffects).effectsSet) {
  override def toString = "AllEffects"
}

object Effects {

  def apply(effectsSeq: Effect*): Effects = Effects(effectsSeq.toSet)

  def propertyRead(expression: Expression, symbols: SymbolTable)(propertyKey: String) = {
    (expression match {
      case i: Identifier => symbols.identifiers.get(i.entityName).map {
        case _: NodeType => Effects(ReadsGivenNodeProperty(propertyKey))
        case _: RelationshipType => Effects(ReadsGivenRelationshipProperty(propertyKey))
        case _ => Effects()
      }
      case _ => None
    }).getOrElse(AllReadEffects)
  }

  def propertyWrite(expression: Expression, symbols: SymbolTable)(propertyKey: String) =
    (expression match {
      case i: Identifier => symbols.identifiers.get(i.entityName).map {
        case _: NodeType => Effects(WritesGivenNodeProperty(propertyKey))
        case _: RelationshipType => Effects(WritesGivenRelationshipProperty(propertyKey))
        case _ => Effects()
      }
      case _ => None
    }).getOrElse(AllWriteEffects)

  implicit class TraversableEffects(iter: Traversable[Effectful]) {
    def effects: Effects = Effects(iter.flatMap(_.effects.effectsSet).toSet)
  }

  implicit class TraversableExpressions(iter: Traversable[Expression]) {
    def effects(symbols: SymbolTable): Effects = Effects(iter.flatMap(_.effects(symbols).effectsSet).toSet)
  }

  implicit class EffectfulReturnItems(iter: Traversable[ReturnItem]) {
    def effects(symbols: SymbolTable): Effects = Effects(iter.flatMap(_.expression.effects(symbols).effectsSet).toSet)
  }

  implicit class EffectfulUpdateAction(commands: Traversable[UpdateAction]) {
    def effects(symbols: SymbolTable): Effects = Effects(commands.flatMap(_.effects(symbols).effectsSet).toSet)
  }

  implicit class MapEffects(m: Map[_, Expression]) {
    def effects(symbols: SymbolTable): Effects = Effects(m.values.flatMap(_.effects(symbols).effectsSet).toSet)
  }

  implicit class SortItemEffects(m: Traversable[SortItem]) {
    def effects(symbols: SymbolTable): Effects = Effects(m.flatMap(_.expression.effects(symbols).effectsSet).toSet)
  }

}

trait Effect {
  def reads: Boolean

  def writes: Boolean

  override def toString = this.getClass.getSimpleName
}

protected trait ReadEffect extends Effect {
  override def reads = true

  override def writes = false

  def toWriteEffect: WriteEffect
}

protected trait WriteEffect extends Effect {
  override def reads = false

  override def writes = true
}

trait ReadsNodes extends ReadEffect

case object ReadsAllNodes extends ReadsNodes {
  override def toWriteEffect = WritesAnyNode
}

case object ReadsRelationshipBoundNodes extends ReadsNodes {
  override def toWriteEffect = WritesRelationshipBoundNodes
}

case class ReadsNodesWithLabels(labels: Set[String]) extends ReadsNodes {
  override def toWriteEffect = WritesNodesWithLabels(labels)
}

object ReadsNodesWithLabels {
  def apply(label: String*): ReadsNodesWithLabels = ReadsNodesWithLabels(label.toSet)
}

trait WritesNodes extends WriteEffect

case object WritesAnyNode extends WritesNodes

case object WritesRelationshipBoundNodes extends WritesNodes

object WritesNodesWithLabels {
  def apply(labels: String*): WritesNodesWithLabels = WritesNodesWithLabels(labels.toSet)
}

case class WritesNodesWithLabels(labels: Set[String]) extends WritesNodes

case object DeletesNode extends WritesNodes

trait ReadsNodeProperty extends ReadEffect

case class ReadsGivenNodeProperty(propertyName: String) extends ReadsNodeProperty {
  override def toString = s"${super.toString} '$propertyName'"

  override def toWriteEffect = WritesGivenNodeProperty(propertyName)
}

object ReadsAnyNodeProperty extends ReadsNodeProperty {
  override def toWriteEffect = WritesAnyNodeProperty

  override def toString = this.getClass.getSimpleName
}

trait WritesNodeProperty extends WriteEffect

case class WritesGivenNodeProperty(propertyName: String) extends WritesNodeProperty {
  override def toString = s"${super.toString} '$propertyName'"
}

object WritesAnyNodeProperty extends WritesNodeProperty {
  override def toString = this.getClass.getSimpleName
}

case object ReadsRelationships extends ReadEffect {
  override def toWriteEffect = WritesRelationships
}

case object WritesRelationships extends WriteEffect

case object DeletesRelationship extends WriteEffect

trait ReadsRelationshipProperty extends ReadEffect

case class ReadsGivenRelationshipProperty(propertyName: String) extends ReadsRelationshipProperty {
  override def toString = s"${super.toString} '$propertyName'"

  override def toWriteEffect = WritesGivenRelationshipProperty(propertyName)
}

object ReadsAnyRelationshipProperty extends ReadsRelationshipProperty {
  override def toWriteEffect = WritesAnyRelationshipProperty

  override def toString = this.getClass.getSimpleName
}

trait WritesRelationshipProperty extends WriteEffect

case class WritesGivenRelationshipProperty(propertyName: String) extends WritesRelationshipProperty {
  override def toString = s"${super.toString} '$propertyName'"
}

object WritesAnyRelationshipProperty extends WritesRelationshipProperty {
  override def toString = this.getClass.getSimpleName
}
