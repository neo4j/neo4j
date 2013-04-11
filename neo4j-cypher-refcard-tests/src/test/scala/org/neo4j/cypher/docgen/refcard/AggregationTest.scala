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
package org.neo4j.cypher.docgen.refcard
import org.neo4j.cypher.{ ExecutionResult, StatisticsChecker }
import org.neo4j.cypher.docgen.RefcardTest

class AggregationTest extends RefcardTest with StatisticsChecker {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val section = "refcard"
  val title = "Aggregation"

  override def assert(name: String, result: ExecutionResult) {
    name match {
      case "returns-one" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
      case "returns-none" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 0)
    }
  }

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=value" =>
        Map("value" -> "Bob")
      case "parameters=percentile" =>
        Map("percentile" -> 0.5)
      case "" =>
        Map()
    }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("propertyName" -> 10),
    "B" -> Map("propertyName" -> 20),
    "C" -> Map("propertyName" -> 30))

  def text = """.Aggregation
[refcard]
----
###assertion=returns-one
START n=node(%A%), m=node(%B%)
MATCH path=(n)-->(m)
RETURN NODES(path),

COUNT(*)
###

The number of matching rows.

###assertion=returns-one
START identifier=node(%A%), m=node(%B%)
MATCH path=(identifier)-->(m)
RETURN NODES(path),

COUNT(identifier)
###

The number of non-`null` values.

###assertion=returns-one
START identifier=node(%A%), m=node(%B%)
MATCH path=(identifier)-->(m)
RETURN NODES(path),

COUNT(DISTINCT identifier)
###

All aggregation functions also take the `DISTINCT` modifier, 
which removes duplicates from the values

###assertion=returns-one
START n=node(%A%, %B%, %C%)
RETURN

SUM(n.propertyName)
###

Sum numerical values.

###assertion=returns-one
START n=node(%A%, %B%, %C%)
RETURN

AVG(n.propertyName)
###

Calculates the average.

###assertion=returns-one
START n=node(%A%, %B%, %C%)
RETURN

MAX(n.propertyName)
###

Maximum numerical value.

###assertion=returns-one
START n=node(%A%, %B%, %C%)
RETURN

MIN(n.propertyName)
###

Minimum numerical value.

###assertion=returns-one
START n=node(%A%, %B%, %C%)
RETURN

COLLECT(n.propertyName?)
###

Collection from the values, ignores `null`.

###assertion=returns-one parameters=percentile
START n=node(%A%, %B%, %C%)
RETURN

PERCENTILE_DISC(n.propertyName, {percentile})
###

Discrete percentile.
The `percentile` argument is from `0.0` to `1.0`.

###assertion=returns-one parameters=percentile
START n=node(%A%, %B%, %C%)
RETURN

PERCENTILE_CONT(n.propertyName, {percentile})
###

Continuous percentile.
----
"""
}
