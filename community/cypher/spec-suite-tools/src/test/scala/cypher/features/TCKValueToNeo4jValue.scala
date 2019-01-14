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
package cypher.features

import org.opencypher.tools.tck.values._

import scala.collection.JavaConverters._

object TCKValueToNeo4jValue extends (CypherValue => Object) {

  def apply(value: CypherValue): Object = {
    value match {
      case CypherString(s) => s
      case CypherInteger(v) => Long.box(v)
      case CypherFloat(v) => Double.box(v)
      case CypherBoolean(v) => Boolean.box(v)
      case CypherProperty(k, v) => (k, TCKValueToNeo4jValue(v))
      case CypherPropertyMap(ps) => ps.map { case (k, v) => k -> TCKValueToNeo4jValue(v) }.asJava
      case l: CypherList => l.elements.map(TCKValueToNeo4jValue).asJava
      case CypherNull => null
      case _ => throw new UnsupportedOperationException(s"Could not convert value $value to a Neo4j representation")
    }
  }

}
