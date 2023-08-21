/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package cypher.features

import org.opencypher.tools.tck.values.CypherBoolean
import org.opencypher.tools.tck.values.CypherFloat
import org.opencypher.tools.tck.values.CypherInteger
import org.opencypher.tools.tck.values.CypherList
import org.opencypher.tools.tck.values.CypherNull
import org.opencypher.tools.tck.values.CypherProperty
import org.opencypher.tools.tck.values.CypherPropertyMap
import org.opencypher.tools.tck.values.CypherString
import org.opencypher.tools.tck.values.CypherValue

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

object TCKValueToNeo4jValue extends (CypherValue => Object) {

  @nowarn("msg=a type was inferred to be `Object`")
  def apply(value: CypherValue): Object = {
    value match {
      case CypherString(s)       => s
      case CypherInteger(v)      => Long.box(v)
      case CypherFloat(v)        => Double.box(v)
      case CypherBoolean(v)      => Boolean.box(v)
      case CypherProperty(k, v)  => (k, TCKValueToNeo4jValue(v))
      case CypherPropertyMap(ps) => ps.map { case (k, v) => k -> TCKValueToNeo4jValue(v) }.asJava
      case l: CypherList         => l.elements.map(TCKValueToNeo4jValue).asJava
      case CypherNull            => null
      case _ => throw new UnsupportedOperationException(s"Could not convert value $value to a Neo4j representation")
    }
  }

}
