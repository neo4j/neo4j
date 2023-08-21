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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RandomValuesTestSupport
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.index.PropertyIndexTestSupport
import org.neo4j.internal.schema.IndexQuery.IndexQueryType.EXISTS
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueCategory

import scala.collection.mutable

abstract class NodeIndexScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime)
    with PropertyIndexTestSupport[CONTEXT]
    with RandomValuesTestSupport {

  testWithIndex(_.supports(EXISTS), "should scan all nodes of an index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(EXISTS))
    val nodes = given {
      nodeIndex(index.indexType, "Honey", "calories")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("calories" -> randomValue(propertyType).asObject())
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(calories)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(_.hasProperty("calories"))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supportsUniqueness(EXISTS), "should scan all nodes of a unique index with a property") { index =>
    val propertyType = randomAmong(index.querySupport(EXISTS))
    val nodes = given {
      uniqueNodeIndex(index.indexType, "Honey", "calories")
      nodeGraph(5, "Milk")
      val seen = mutable.Set.empty[Value]
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 =>
            val value = randomValue(propertyType)
            if (seen.add(value)) Map("calories" -> value.asObject()) else Map.empty
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(calories)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(_.hasProperty("calories"))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(
    _.supportsComposite(EXISTS, ValueCategory.NUMBER, ValueCategory.NUMBER),
    "should scan all nodes of an index with multiple properties"
  ) { index =>
    val nodes = given {
      nodeIndex(index.indexType, "Honey", "calories", "taste")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("calories" -> i, "taste" -> i)
          case i if i % 5 == 0  => Map("calories" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator("x:Honey(calories,taste)", indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.filter(n => n.hasProperty("calories") && n.hasProperty("taste"))
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  testWithIndex(_.supportsValues(EXISTS), "should cache properties") { index =>
    val propertyType = randomAmong(index.provideValueSupport(EXISTS))
    val nodes = given {
      nodeIndex(index.indexType, "Honey", "calories")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("calories" -> randomValue(propertyType).asObject())
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "calories")
      .projection("cache[x.calories] AS calories")
      .nodeIndexOperator("x:Honey(calories)", _ => GetValue, indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.collect {
      case n if n.hasProperty("calories") => Array(n, n.getProperty("calories"))
    }
    runtimeResult should beColumns("x", "calories").withRows(expected)
  }

  testWithIndex(_.supportsUniqueness(EXISTS), "should cache properties with a unique index") { index =>
    val propertyType = randomAmong(index.provideValueSupport(EXISTS))
    val nodes = given {
      uniqueNodeIndex(index.indexType, "Honey", "calories")
      nodeGraph(5, "Milk")
      val seen = mutable.Set.empty[Value]
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 =>
            val value = randomValue(propertyType)
            if (seen.add(value)) Map("calories" -> value.asObject()) else Map.empty
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "calories")
      .projection("cache[x.calories] AS calories")
      .nodeIndexOperator("x:Honey(calories)", _ => GetValue, indexType = index.indexType)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.collect {
      case n if n.hasProperty("calories") => Array(n, n.getProperty("calories"))
    }
    runtimeResult should beColumns("x", "calories").withRows(expected)
  }
}
