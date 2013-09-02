/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.helpers.IsMap
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.commands.values.KeyToken

import org.neo4j.graphdb.NotFoundException
import org.neo4j.cypher.EntityNotFoundException
import scala.runtime.ScalaRunTime

object Property {
  def apply(mapExpr: Expression, propertyKey: KeyToken) = new Property(mapExpr, propertyKey, true)
  def unapply(property: Property) = Some((property.mapExpr, property.propertyKey))
}

class Property(val mapExpr: Expression,
               val propertyKey: KeyToken,
               val nullOnNotFound: Boolean /* Required only for Cypher 1.9 */)
  extends Expression with Product with Serializable
{
  def copy(mapExpr: Expression = this.mapExpr, propertyKey: KeyToken = this.propertyKey, nullOnNotFound: Boolean = this.nullOnNotFound) =
    new Property(mapExpr, propertyKey, nullOnNotFound)
  override def productPrefix = classOf[Product].getSimpleName
  def productArity = 3
  def productElement(n: Int): Any = n match {
    case 0 => this.mapExpr
    case 1 => this.propertyKey
    case 2 => this.nullOnNotFound
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  def canEqual(that: Any) = that.isInstanceOf[Property]
  override def equals(that: Any) = canEqual(that) && {
    val other = that.asInstanceOf[Property]
    this.mapExpr == other.mapExpr && this.propertyKey == other.propertyKey
  }
  override def hashCode() = this.mapExpr.hashCode * this.propertyKey.hashCode

  override def toString = ScalaRunTime._toString(this)

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = mapExpr(ctx) match {
    case null           => null
    case IsMap(mapFunc) => try {
      mapFunc(state.query).apply(propertyKey.name)
    } catch {
      case _: EntityNotFoundException if nullOnNotFound => null
      case _: NotFoundException if nullOnNotFound => null
    }
    case _              => throw new ThisShouldNotHappenError("Andres", "Need something with properties")
  }

  def rewrite(f: (Expression) => Expression) = f(new Property(mapExpr.rewrite(f), propertyKey.rewrite(f), nullOnNotFound))

  def children = Seq(mapExpr)

  def calculateType(symbols: SymbolTable) =
    throw new ThisShouldNotHappenError("Andres", "This class should override evaluateType, and this method should never be run")

  override def evaluateType(expectedType: CypherType, symbols: SymbolTable) = {
    mapExpr.evaluateType(MapType(), symbols)
    expectedType
  }

  def symbolTableDependencies = mapExpr.symbolTableDependencies
}
