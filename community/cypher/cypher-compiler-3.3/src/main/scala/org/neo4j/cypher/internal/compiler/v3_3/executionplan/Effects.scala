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
package org.neo4j.cypher.internal.compiler.v3_3.executionplan

import org.neo4j.cypher.internal.compiler.v3_3.commands.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.compiler.v3_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

case class Effects(effectsSet: Set[Effect] = Set.empty) {

  def writeEffects: Effects = Effects(effectsSet.collect[Effect, Set[Effect]] {
    case write: WriteEffect => write
  })

  def ++(other: Effects): Effects = Effects(effectsSet ++ other.effectsSet)

  def contains(effect: Effect): Boolean = effectsSet(effect)

  def containsWrites = effectsSet.exists {
    case write: WriteEffect => true
    case _ => false
  }

  def containsNodeReads = effectsSet.exists {
    case _: ReadsNodes => true
    case _ => false
  }

  def containsRelationshipReads = effectsSet.exists {
    case _: ReadsRelationships => true
    case ReadsRelationshipBoundNodes => true
    case _ => false
  }

  def regardlessOfLeafEffects = Effects(effectsSet.map {
    case LeafEffect(e) => e
    case e => e
  })

  def regardlessOfOptionalEffects = Effects(effectsSet.map {
    case OptionalLeafEffect(e) => e
    case e => e
  })

  def asLeafEffects = Effects(effectsSet.map[Effect, Set[Effect]] {
    effect: Effect => LeafEffect(effect)
  })

  def leafEffectsAsOptional = Effects(effectsSet.map {
    case LeafEffect(e) => OptionalLeafEffect(e)
    case e => e
  })
}

object AllWriteEffects extends Effects(Set(CreatesAnyNode, WriteAnyNodeProperty, WriteAnyRelationshipProperty))

object AllReadEffects extends Effects(Set(ReadsAllNodes, ReadsAllRelationships, ReadsAnyNodeProperty, ReadsAnyRelationshipProperty))

object AllEffects extends Effects((AllWriteEffects ++ AllReadEffects).effectsSet)

object Effects {

  def apply(effectsSeq: Effect*): Effects = Effects(effectsSeq.toSet)

  def propertyRead(expression: Expression, symbols: SymbolTable)(propertyKey: String) = {
    (expression match {
      case i: Variable => symbols.variables.get(i.entityName).map {
        case _: NodeType => Effects(ReadsGivenNodeProperty(propertyKey))
        case _: RelationshipType => Effects(ReadsGivenRelationshipProperty(propertyKey))
        case _ => Effects()
      }
      case _ => None
    }).getOrElse(Effects())
  }

  def propertyWrite(expression: Expression, symbols: SymbolTable)(propertyKey: String) =
    (expression match {
      case i: Variable => symbols.variables.get(i.entityName).map {
        case _: NodeType => Effects(SetGivenNodeProperty(propertyKey))
        case _: RelationshipType => Effects(SetGivenRelationshipProperty(propertyKey))
        case _ => Effects()
      }
      case _ => None
    }).getOrElse(Effects())
}

//-----------------------------------------------------------------------------
// Effect
//-----------------------------------------------------------------------------
sealed trait Effect

case class LeafEffect(effect: Effect) extends Effect

case class OptionalLeafEffect(effect: Effect) extends Effect

//-----------------------------------------------------------------------------
// Read effects
//-----------------------------------------------------------------------------
sealed trait ReadEffect extends Effect

sealed trait ReadsNodes extends ReadEffect

case object ReadsAllNodes extends ReadsNodes

case object ReadsRelationshipBoundNodes extends ReadsNodes

case class ReadsNodesWithLabels(labels: Set[String]) extends ReadsNodes

object ReadsNodesWithLabels {
  def apply(label: String*): ReadsNodesWithLabels = ReadsNodesWithLabels(label.toSet)
}

sealed trait ReadsNodeProperty extends ReadEffect

case class ReadsGivenNodeProperty(propertyName: String) extends ReadsNodeProperty

case object ReadsAnyNodeProperty extends ReadsNodeProperty

sealed trait ReadsRelationships extends ReadEffect

case object ReadsAllRelationships extends ReadsRelationships

case class ReadsRelationshipsWithTypes(types: Set[String]) extends ReadsRelationships

object ReadsRelationshipsWithTypes {

  def apply(types: String*): ReadsRelationships =
    if (types.isEmpty) ReadsAllRelationships
    else ReadsRelationshipsWithTypes(types.toSet)
}

sealed trait ReadsRelationshipProperty extends ReadEffect

case class ReadsGivenRelationshipProperty(propertyName: String) extends ReadsRelationshipProperty

case object ReadsAnyRelationshipProperty extends ReadsRelationshipProperty

case object ReadsAnyLabel extends ReadEffect

//-----------------------------------------------------------------------------
// Write effects
//-----------------------------------------------------------------------------
sealed trait WriteEffect extends Effect

case class SetLabel(label: String) extends WriteEffect

sealed trait WritesNodes extends WriteEffect

case object DeletesNode extends WritesNodes

sealed trait CreatesNodes extends WritesNodes

case object CreatesAnyNode extends CreatesNodes

case object CreatesRelationshipBoundNodes extends CreatesNodes

case class CreatesNodesWithLabels(labels: Set[String]) extends CreatesNodes

object CreatesNodesWithLabels {
  def apply(labels: String*): CreatesNodesWithLabels = CreatesNodesWithLabels(labels.toSet)
}

sealed trait WriteNodeProperty extends WriteEffect

case class SetGivenNodeProperty(propertyName: String) extends WriteNodeProperty

case object WriteAnyNodeProperty extends WriteNodeProperty // Set, Remove

case object DeletesRelationship extends WriteEffect

sealed trait CreatesRelationships extends WriteEffect

case class CreatesRelationship(typ: String) extends CreatesRelationships

sealed trait WriteRelationshipProperty extends WriteEffect

case class SetGivenRelationshipProperty(propertyName: String) extends WriteRelationshipProperty

case object WriteAnyRelationshipProperty extends WriteRelationshipProperty // Set, Remove
