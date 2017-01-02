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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_2._
import symbols._

case class Collection(expressions: Seq[Expression])(val position: InputPosition) extends Expression {
  def semanticCheck(ctx: SemanticContext) = expressions.semanticCheck(ctx) chain specifyType(possibleTypes)

  private def possibleTypes: TypeGenerator = state => expressions match {
    case Seq() => CTCollection(CTAny).covariant
    case _     => expressions.leastUpperBoundsOfTypes(state).wrapInCollection
  }
}

case class CollectionSlice(collection: Expression, from: Option[Expression], to: Option[Expression])(val position: InputPosition)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) chain
    collection.expectType(CTCollection(CTAny).covariant) chain
    when(from.isEmpty && to.isEmpty) {
      SemanticError("The start or end (or both) is required for a collection slice", position)
    } chain
    from.semanticCheck(ctx) chain
    from.expectType(CTInteger.covariant) chain
    to.semanticCheck(ctx) chain
    to.expectType(CTInteger.covariant) chain
    specifyType(collection.types)
}

case class CollectionIndex(collection: Expression, idx: Expression)(val position: InputPosition)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) chain
    collection.expectType(CTCollection(CTAny).covariant) chain
    idx.semanticCheck(ctx) chain
    idx.expectType(CTInteger.covariant) chain
    specifyType(collection.types(_).unwrapCollections)
}
