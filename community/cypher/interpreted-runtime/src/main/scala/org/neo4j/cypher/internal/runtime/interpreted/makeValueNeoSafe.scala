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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{ArrayValue, Value, Values}
import org.neo4j.values.virtual.ListValue

import scala.collection.JavaConverters._

object makeValueNeoSafe extends (AnyValue => Value) with ListSupport {

  def apply(a: AnyValue): Value = a match {
    case value: Value => value
    case IsList(l) => transformTraversableToArray(l)
    case _ => throw new CypherTypeException("Property values can only be of primitive types or arrays thereof")
  }
  /*
  This method finds the type that we can use for the primitive array that Neo4j wants
  We can't just find the nearest common supertype - we need a type that the other values
  can be coerced to according to Cypher coercion rules
   */
  private def transformTraversableToArray(a: ListValue): ArrayValue = {
    if (a.storable()) {
      a.toStorableArray
    } else if (a.isEmpty) {
      Values.stringArray(Array.empty[String]:_*)
    } else {
      val typeValue = a.iterator().asScala.reduce(CastSupport.merge)
      val converter = CastSupport.getConverter(typeValue)
      converter.arrayConverter(a)
    }
  }
}
