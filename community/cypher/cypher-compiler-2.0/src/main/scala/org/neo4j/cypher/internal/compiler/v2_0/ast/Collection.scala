/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._

case class Collection(expressions: Seq[Expression])(val position: InputPosition) extends Expression {
  def semanticCheck(ctx: SemanticContext) = expressions.semanticCheck(ctx) then specifyType(possibleTypes)

  private def possibleTypes: TypeGenerator = state => expressions match {
    case Seq() => CTCollection(CTAny).covariant
    case _     => expressions.leastUpperBoundsOfTypes(state).wrapInCollection
  }
}

case class CollectionSlice(collection: Expression, from: Option[Expression], to: Option[Expression])(val position: InputPosition)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) then
    collection.expectType(CTCollection(CTAny).covariant) then
    when(from.isEmpty && to.isEmpty) {
      SemanticError("The start or end (or both) is required for a collection slice", position)
    } then
    from.semanticCheck(ctx) then
    from.expectType(CTInteger.covariant) then
    to.semanticCheck(ctx) then
    to.expectType(CTInteger.covariant) then
    specifyType(collection.types)
}

case class CollectionIndex(collection: Expression, idx: Expression)(val position: InputPosition)
  extends Expression {

  override def semanticCheck(ctx: SemanticContext) =
    collection.semanticCheck(ctx) then
    collection.expectType(CTCollection(CTAny).covariant) then
    idx.semanticCheck(ctx) then
    idx.expectType(CTInteger.covariant) then
    specifyType(collection.types(_).unwrapCollections)
}
