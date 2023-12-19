/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
