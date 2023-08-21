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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.cypher.testing.api.ParameterConverter
import org.neo4j.driver
import org.neo4j.graphdb.spatial.Point

import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount

object DriverParameterConverter extends ParameterConverter {

  def convertParameterValue(value: Any): AnyRef = value match {
    case null                         => null
    case map: Map[_, _]               => convertParameters(map.asInstanceOf[Map[String, Any]])
    case array: Array[AnyRef]         => array.map(convertParameterValue)
    case iterable: Iterable[_]        => iterable.map(convertParameterValue).toArray
    case traversable: IterableOnce[_] => traversable.map(convertParameterValue).toArray
    case d: TemporalAmount            => convertTemporalValue(d)
    case _: Point                     => throw new IllegalStateException("Point type is not supported yet")
    case x                            => x.asInstanceOf[AnyRef]
  }

  private def convertTemporalValue(serverValue: TemporalAmount): driver.Value =
    driver.Values.isoDuration(
      serverValue.get(ChronoUnit.MONTHS),
      serverValue.get(ChronoUnit.DAYS),
      serverValue.get(ChronoUnit.SECONDS),
      serverValue.get(ChronoUnit.NANOS).toInt
    )
}
