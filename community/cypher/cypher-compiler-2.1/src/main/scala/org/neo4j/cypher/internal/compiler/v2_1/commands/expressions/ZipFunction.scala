/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_1._
import pipes.QueryState
import symbols._
import org.neo4j.cypher.internal.helpers._

case class ZipFunction(collection1: Expression, collection2: Expression)
  extends NullInNullOutExpression(collection1) with CollectionSupport {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = {
    if(collection2(m) == null) null
    else makeTraversable(collection1(m)).zip(makeTraversable(collection2(m))).map(_.productIterator.toList)
  }

  def rewrite(f: (Expression) => Expression) =
    f(ZipFunction(collection1.rewrite(f), collection2.rewrite(f)))

  def arguments: Seq[Expression] = Seq(collection1, collection2)

  override def children = Seq(collection1, collection2)

  def identifierDependencies(expectedType: CypherType) = AnyType

  def calculateType(symbols: SymbolTable) = {
    CTCollection(CTCollection(collection1.evaluateType(CTCollection(CTAny), symbols).mergeUp(collection2.evaluateType(CTCollection(CTAny), symbols))))
  }

  def symbolTableDependencies = (collection1.symbolTableDependencies ++ collection2.symbolTableDependencies)
}
