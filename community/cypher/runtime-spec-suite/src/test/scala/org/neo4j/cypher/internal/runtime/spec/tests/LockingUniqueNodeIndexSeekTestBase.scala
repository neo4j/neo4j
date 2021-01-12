/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.lock.LockType.EXCLUSIVE
import org.neo4j.lock.LockType.SHARED
import org.neo4j.lock.ResourceTypes.INDEX_ENTRY
import org.neo4j.lock.ResourceTypes.LABEL

import scala.util.Random

abstract class LockingUniqueNodeIndexSeekTestBase[CONTEXT <: RuntimeContext](
                                                                              edition: Edition[CONTEXT],
                                                                              runtime: CypherRuntime[CONTEXT],
                                                                              val sizeHint: Int
                                                                            ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should grab shared lock when finding a node") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      }, "Honey")
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop = $propToFind)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(propToFind)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should grab an exclusive lock when not finding a node") {
    val nodes = given {
      uniqueIndex("Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      }, "Honey")
    }
    val propToFind = sizeHint + 1


    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeIndexOperator(s"x:Honey(prop = $propToFind)", unique = true)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows().withLocks((EXCLUSIVE, INDEX_ENTRY), (SHARED, LABEL))
  }
}
