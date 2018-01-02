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
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CastSupport, CollectionSupport, IsCollection, IsMap}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class ContainerIndex(expression: Expression, index: Expression) extends NullInNullOutExpression(expression)
with CollectionSupport {
  def arguments = Seq(expression, index)

  def compute(value: Any, ctx: ExecutionContext)(implicit state: QueryState): Any = {
    value match {
      case IsMap(m) =>
        val item = index(ctx)
        if (item == null) null
        else {
          val key = CastSupport.castOrFail[String](item)
          m(state.query).getOrElse(key, null)
        }

      case IsCollection(collection) =>
        val item = index(ctx)
        if (item == null) null
        else {
          var idx = CastSupport.castOrFail[Number](item).intValue()
          val collectionValue = collection.toVector

          if (idx < 0)
            idx = collectionValue.size + idx

          if (idx >= collectionValue.size || idx < 0) null
          else collectionValue.apply(idx)
        }

      case _ =>
        throw new CypherTypeException(s"""
           |Element access is only possible by performing a collection lookup using an integer index,
           |or by performing a map lookup using a string key (found: $value[${index(ctx)}])""".stripMargin)
    }
  }

  def calculateType(symbols: SymbolTable): CypherType = {
    val exprT = expression.evaluateType(CTAny, symbols)
    val indexT = index.evaluateType(CTAny, symbols)

    val isColl = CTCollection(CTAny).isAssignableFrom(exprT)
    val isMap = CTMap.isAssignableFrom(exprT)
    val isInteger = CTInteger.isAssignableFrom(indexT)
    val isString = CTString.isAssignableFrom(indexT)

    val collectionLookup = isColl || isInteger
    val mapLookup = isMap || isString

    if (collectionLookup && !mapLookup) {
      index.evaluateType(CTInteger, symbols)
      expression.evaluateType(CTCollection(CTAny), symbols) match {
        case collectionType: CollectionType => collectionType.innerType
        case x if x.isInstanceOf[AnyType]   => CTAny
        case x                              => throw new CypherTypeException("Expected a collection, but was " + x)
      }
    } else if (!collectionLookup && mapLookup) {
      index.evaluateType(CTString, symbols)
      expression.evaluateType(CTMap, symbols) match {
        case t: MapType                   => CTAny
        case t: NodeType                  => CTAny
        case t: RelationshipType          => CTAny
        case x if x.isInstanceOf[AnyType] => CTAny
        case x                            => throw new CypherTypeException("Expected a map, but was " + x)
      }
    } else {
      CTAny
    }
  }

  def rewrite(f: (Expression) => Expression): Expression = f(ContainerIndex(expression.rewrite(f), index.rewrite(f)))

  def symbolTableDependencies: Set[String] = expression.symbolTableDependencies ++ index.symbolTableDependencies
}
