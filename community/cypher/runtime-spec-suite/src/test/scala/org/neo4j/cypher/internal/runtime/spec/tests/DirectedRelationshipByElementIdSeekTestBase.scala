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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.util.Random

abstract class DirectedRelationshipByElementIdSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val random = new Random(77)

  private def quote(s: String): String = s"'$s'"

  test("should find single relationship") {
    // given
    val (_, relationships) = given { circleGraph(17) }
    val relToFind = relationships(random.nextInt(relationships.length))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .directedRelationshipByElementIdSeek("r", "x", "y", Set.empty, quote(relToFind.getElementId))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(singleRow(
      relToFind,
      relToFind.getStartNode,
      relToFind.getEndNode
    ))
  }

  test("should not find non-existing relationship") {
    // given
    given { circleGraph(17) }
    val toNotFind = "bad-id"

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .directedRelationshipByElementIdSeek("r", "x", "y", Set.empty, quote(toNotFind))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("should find multiple relationships") {
    // given
    val (_, relationships) = given { circleGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .directedRelationshipByElementIdSeek("r", "x", "y", Set.empty, toFind.map(_.getElementId).map(quote): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = toFind.map(r => Array(r, r.getStartNode, r.getEndNode))
    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should find some relationships and not others") {
    // given
    val (_, relationships) = given { circleGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    val toNotFind1 = relationships.last.getElementId.drop(1)
    val toNotFind2 = toNotFind1.drop(1)
    val relationshipsToLookFor = toNotFind1 +: toFind.map(_.getElementId) :+ toNotFind2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .directedRelationshipByElementIdSeek("r", "x", "y", Set.empty, relationshipsToLookFor.map(quote): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = toFind.map(r => Array(r, r.getStartNode, r.getEndNode))
    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should handle relById + filter") {
    // given
    val (_, relationships) = given { circleGraph(sizeHint) }
    val toSeekFor = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    val toFind = toSeekFor(random.nextInt(toSeekFor.length))
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter(s"elementId(r) = '${toFind.getElementId}'")
      .directedRelationshipByElementIdSeek("r", "x", "y", Set.empty, toSeekFor.map(_.getElementId).map(quote): _*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq(Array(toFind, toFind.getStartNode, toFind.getEndNode)))
  }
}
