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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_3.mutation.MapPropertySetAction
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.graphdb.PropertyContainer

class MapPropertySetActionTest extends GraphDatabaseFunSuite with QueryStateTestSupport {

  test("set single value on node") {
    val a = createNode()
    val m = Map("meaning_of_life" -> 420)

    withCountsQueryState { queryState =>
      setProperties(a, m)(queryState)

      queryState.getStatistics.propertiesSet should equal(1)
      a.getProperty("meaning_of_life") should equal(420)
    }
  }

  test("set multiple properties") {
    val a = createNode()
    val m = Map("A" -> 1, "b" -> 2)

    withCountsQueryState { queryState =>
      setProperties(a, m)(queryState)

      queryState.getStatistics.propertiesSet should equal(2)
      a.getProperty("A") should equal(1)
      a.getProperty("b") should equal(2)
    }
  }

  test("set properties on relationship") {
    val a = createNode()
    val r = relate(createNode(), a)

    val m = Map("A" -> 1, "b" -> 2)

    withCountsQueryState { queryState =>
      setProperties(a, m)(queryState)

      queryState.getStatistics.propertiesSet should equal(2)
      a.getProperty("A") should equal(1)
      a.getProperty("b") should equal(2)
    }
  }

  test("transfer properties from node to node") {
    val from = createNode("foo" -> "bar", "buzz" -> 42)
    val to = createNode()


    withCountsQueryState { queryState =>
      setProperties(to, from)(queryState)

      queryState.getStatistics.propertiesSet should equal(2)
      to.getProperty("foo") should equal("bar")
      to.getProperty("buzz") should equal(42)
    }
  }

  test("remove properties from node") {
    val from = Map("a" -> 1)
    val to = createNode("b" -> 2)

    withCountsQueryState { queryState =>
      setProperties(to, from)(queryState)

      queryState.getStatistics.propertiesSet should equal(2)
      to.getProperty("a") should equal(1)
      to.hasProperty("b") should equal(false)
    }
  }

  test("should overwrite values") {
    val from = Map("a" -> 1)
    val to = createNode("a" -> "apa")

    withCountsQueryState { queryState =>
      setProperties(to, from)(queryState)

      queryState.getStatistics.propertiesSet should equal(1)
      to.getProperty("a") should equal(1)
    }
  }

  test("should overwrite but keep other values") {
    val from = Map("a" -> 1)
    val to = createNode("a" -> "apa", "b" -> "apa")

    withCountsQueryState { queryState =>
      setPropertiesWithoutCleaning(to, from)(queryState)

      queryState.getStatistics.propertiesSet should equal(1)
      to.getProperty("a") should equal(1)
      to.getProperty("b") should equal("apa")
    }
  }

  test("explicit null removes values") {
    val from = Map[String, Any]("a" -> 1, "b" -> null)
    val to = createNode("a" -> "A", "b" -> "B", "c" -> "C")

    withCountsQueryState { queryState =>
      setPropertiesWithoutCleaning(to, from)(queryState)

      queryState.getStatistics.propertiesSet should equal(2)
      to.getProperty("a") should equal(1)
      to.hasProperty("b") should equal(false)
    }
  }

  private def setProperties(a: PropertyContainer, m: Any) = (queryState: QueryState) => {
    val setter = MapPropertySetAction(Literal(a), Literal(m), removeOtherProps = true)
    setter.exec(ExecutionContext.empty, queryState)
  }

  private def setPropertiesWithoutCleaning(a: PropertyContainer, m: Any) = (queryState: QueryState) =>  {
    val setter = MapPropertySetAction(Literal(a), Literal(m), removeOtherProps = false)
    setter.exec(ExecutionContext.empty, queryState)
  }
}
