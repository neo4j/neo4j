/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.IsList
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.values._
import org.neo4j.values.storable.{UTF8StringValue, _}
import org.neo4j.values.virtual.VirtualValues

case class Add(a: Expression, b: Expression) extends Arithmetics(a, b) {
  override def applyWithValues(aVal: AnyValue, bVal: AnyValue): AnyValue = {
    (aVal, bVal) match {
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
      case (x: NumberValue, y: NumberValue) => x.plus(y)
      case (x: UTF8StringValue, y: UTF8StringValue) => x.plus(y)
      case (x: TextValue, y: TextValue) => Values.stringValue(x.stringValue() + y.stringValue())
      case (IsList(x), IsList(y)) => VirtualValues.concat(x, y)
      case (IsList(x), y)         => VirtualValues.appendToList(x, y)
      case (x, IsList(y))         => VirtualValues.prependToList(y, x)
      case (x: TextValue, y: IntegralValue) => Values.stringValue(x.stringValue() + y.longValue())
      case (x: IntegralValue, y: TextValue) => Values.stringValue(x.longValue() + y.stringValue())
      case (x: TextValue, y: FloatValue) => Values.stringValue(x.stringValue() + y.doubleValue())
      case (x: FloatValue, y: TextValue) => Values.stringValue(x.doubleValue() + y.stringValue())
      case (x: TemporalValue[_,_], y: DurationValue) => x.plus(y)
      case (x: DurationValue, y: TemporalValue[_,_]) => y.plus(x)
      case (x: DurationValue, y: DurationValue) => x.add(y)
      case _                      => throwTypeError(aVal.getTypeName, bVal.getTypeName)
    }
  }

  def rewrite(f: (Expression) => Expression) = f(Add(a.rewrite(f), b.rewrite(f)))

  private def mergeWithCollection(collection: CypherType, singleElement: CypherType):CypherType= {
    val collectionType = collection.asInstanceOf[ListType]
    val mergedInnerType = collectionType.innerType.leastUpperBound(singleElement)
    CTList(mergedInnerType)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies

  override def throwTypeError(aType: String, bType: String): Nothing = {
    throw new CypherTypeException("Cannot add `" + aType + "` and `" + bType + "`")
  }

  override def calc(a: NumberValue, b: NumberValue): AnyValue = a.plus(b)
}
