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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

import scala.util.Random

abstract class UndirectedRelationshipByIdSeekTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val random = new Random(77)

  test("should find single relationship") {
    // given
    val (_, relationships) = given { circleGraph(17) }
    val relToFind = relationships(random.nextInt(relationships.length))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", relToFind.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq(
      Array(relToFind, relToFind.getStartNode, relToFind.getEndNode),
      Array(relToFind, relToFind.getEndNode, relToFind.getStartNode)))
  }

  test("should find by floating point") {
    // given
    val (_, Seq(rel, _)) = given { circleGraph(2) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .undirectedRelationshipByIdSeek("r", "x", "y", rel.getId.toDouble)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(rel, rel)))
  }

  test("should not find non-existing relationship") {
    // given
    val (_, relationships) = given { circleGraph(17) }
    val toNotFind = relationships.map(_.getId).max + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", toNotFind)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should  beColumns("r", "x", "y").withNoRows()
  }

  test("should find multiple relationships") {
    // given
    val (_, relationships) = given { circleGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    restartTx()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", toFind.map(_.getId):_*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = toFind.flatMap(r => {
      val rel = tx.getRelationshipById(r.getId)
      Seq(Array(rel, rel.getStartNode, rel.getEndNode), Array(rel, rel.getEndNode, rel.getStartNode))
    })
    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should find some relationships and not others") {
    // given
    val (_, relationships) = given { circleGraph(sizeHint) }
    val toFind = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    val toNotFind1 = relationships.map(_.getId).max + 1
    val toNotFind2 = toNotFind1 + 1
    val relationshipsToLookFor = toNotFind1 +: toFind.map(_.getId) :+ toNotFind2
    restartTx()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .undirectedRelationshipByIdSeek("r", "x", "y", relationshipsToLookFor:_*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = toFind.flatMap(r => {
      val rel = tx.getRelationshipById(r.getId)
      Seq(Array(rel, rel.getStartNode, rel.getEndNode), Array(rel, rel.getEndNode, rel.getStartNode))
    })
    runtimeResult should beColumns("r", "x", "y").withRows(expected)
  }

  test("should handle relById + filter") {
    // given
    val (_, relationships) = given { circleGraph(sizeHint) }
    val toSeekFor = (1 to 5).map(_ => relationships(random.nextInt(relationships.length)))
    val toFind = toSeekFor(random.nextInt(toSeekFor.length))
    restartTx()

    val attachedToFind = tx.getRelationshipById(toFind.getId)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter(s"id(r) = ${attachedToFind.getId}")
      .undirectedRelationshipByIdSeek("r", "x", "y", toSeekFor.map(_.getId):_*)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(Seq(
      Array(attachedToFind, attachedToFind.getStartNode, attachedToFind.getEndNode),
      Array(attachedToFind, attachedToFind.getEndNode, attachedToFind.getStartNode)))
  }
}
