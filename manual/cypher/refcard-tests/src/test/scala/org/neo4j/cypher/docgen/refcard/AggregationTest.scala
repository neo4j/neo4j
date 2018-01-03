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
package org.neo4j.cypher.docgen.refcard

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.{ ExecutionResult, QueryStatisticsTestSupport }
import org.neo4j.cypher.docgen.RefcardTest

class AggregationTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("ROOT KNOWS A", "A KNOWS B", "B KNOWS C", "C KNOWS ROOT")
  val title = "Aggregation"
  val css = "general c3-3 c4-4 c5-4 c6-6"
  override val linkId = "query-aggregation"

  override def assert(name: String, result: InternalExecutionResult) {
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
    "A" -> Map("property" -> 10),
    "B" -> Map("property" -> 20),
    "C" -> Map("property" -> 30))

  def text = """
###assertion=returns-one
MATCH path = (n)-->(m)
WHERE id(n) = %A% AND id(m) = %B%
RETURN NODES(path),

count(*)
###

The number of matching rows.

###assertion=returns-one
MATCH path = (identifier)-->(m)
WHERE id(identifier) = %A% AND id(m) = %B%
RETURN nodes(path),

count(identifier)
###

The number of non-++NULL++ values.

###assertion=returns-one
MATCH path = (identifier)-->(m)
WHERE id(identifier) = %A% AND id(m) = %B%
RETURN nodes(path),

count(DISTINCT identifier)
###

All aggregation functions also take the `DISTINCT` modifier,
which removes duplicates from the values.

###assertion=returns-one
MATCH n WHERE id(n) IN [%A%, %B%, %C%]
RETURN

collect(n.property)
###

Collection from the values, ignores `NULL`.

###assertion=returns-one
MATCH n WHERE id(n) IN [%A%, %B%, %C%]
RETURN

sum(n.property)

,avg(n.property), min(n.property), max(n.property)
###

Sum numerical values. Similar functions are +avg+, +min+, +max+.

###assertion=returns-one parameters=percentile
MATCH n WHERE id(n) IN [%A%, %B%, %C%]
RETURN

percentileDisc(n.property, {percentile})

,percentileCont(n.property, {percentile})
###

Discrete percentile. Continuous percentile is +percentileCont+.
The `percentile` argument is from `0.0` to `1.0`.

###assertion=returns-one parameters=percentile
MATCH n WHERE id(n) IN [%A%, %B%, %C%]
RETURN

stdev(n.property)

, stdevp(n.property)
###

Standard deviation for a sample of a population.
For an entire population use +stdevp+.
"""
}
