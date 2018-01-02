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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, InternalException, SemanticCheckable}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

sealed trait Hint extends ASTNode with ASTPhrase with SemanticCheckable {
  def identifiers: NonEmptyList[Identifier]
}

trait NodeHint {
  self: Hint =>
}

trait RelationshipHint {
  self: Hint =>
}

object Hint {
  implicit val byIdentifier: Ordering[Hint] =
    Ordering.by { (hint: Hint) => hint.identifiers.head }(Identifier.byName)
}
// allowed on match

sealed trait UsingHint extends Hint

// allowed on start item

sealed trait LegacyIndexHint extends Hint {
  self: StartItem =>

  def identifier: Identifier
  def identifiers = NonEmptyList(identifier)
}

case class UsingIndexHint(identifier: Identifier, label: LabelName, property: PropertyKeyName)(val position: InputPosition) extends UsingHint with NodeHint {
  def identifiers = NonEmptyList(identifier)
  def semanticCheck = identifier.ensureDefined chain identifier.expectType(CTNode.covariant)
}

case class UsingScanHint(identifier: Identifier, label: LabelName)(val position: InputPosition) extends UsingHint with NodeHint {
  def identifiers = NonEmptyList(identifier)
  def semanticCheck = identifier.ensureDefined chain identifier.expectType(CTNode.covariant)
}

object UsingJoinHint {
  import NonEmptyList._

  def apply(elts: Seq[Identifier])(pos: InputPosition): UsingJoinHint =
    UsingJoinHint(elts.toNonEmptyListOption.getOrElse(throw new InternalException("Expected non-empty sequence of identifiers")))(pos)
}

case class UsingJoinHint(identifiers: NonEmptyList[Identifier])(val position: InputPosition) extends UsingHint with NodeHint {
  def semanticCheck =
    identifiers.map { identifier => identifier.ensureDefined chain identifier.expectType(CTNode.covariant) }.reduceLeft(_ chain _)
}

// start items

sealed trait StartItem extends ASTNode with ASTPhrase with SemanticCheckable {
  def identifier: Identifier
  def name = identifier.name
}

sealed trait NodeStartItem extends StartItem {
  def semanticCheck = identifier.declare(CTNode)
}

case class NodeByIdentifiedIndex(identifier: Identifier, index: String, key: String, value: Expression)(val position: InputPosition)
  extends NodeStartItem with LegacyIndexHint with NodeHint

case class NodeByIndexQuery(identifier: Identifier, index: String, query: Expression)(val position: InputPosition)
  extends NodeStartItem with LegacyIndexHint with NodeHint

case class NodeByParameter(identifier: Identifier, parameter: Parameter)(val position: InputPosition) extends NodeStartItem
case class AllNodes(identifier: Identifier)(val position: InputPosition) extends NodeStartItem

sealed trait RelationshipStartItem extends StartItem {
  def semanticCheck = identifier.declare(CTRelationship)
}

case class RelationshipByIds(identifier: Identifier, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByParameter(identifier: Identifier, parameter: Parameter)(val position: InputPosition) extends RelationshipStartItem
case class AllRelationships(identifier: Identifier)(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByIdentifiedIndex(identifier: Identifier, index: String, key: String, value: Expression)(val position: InputPosition) extends RelationshipStartItem with LegacyIndexHint with RelationshipHint
case class RelationshipByIndexQuery(identifier: Identifier, index: String, query: Expression)(val position: InputPosition) extends RelationshipStartItem with LegacyIndexHint with RelationshipHint

// no longer supported non-hint legacy start items

case class NodeByIds(identifier: Identifier, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends NodeStartItem

