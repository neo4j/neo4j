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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherPipelinedInterpretedPipesFallback
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.exceptions.HintException

abstract class SlottedPipeFallbackTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition.copyWith(
        // Set this to allow support for nonPipelined()
        GraphDatabaseInternalSettings.cypher_pipelined_interpreted_pipes_fallback -> CypherPipelinedInterpretedPipesFallback.ALL
      ),
      runtime
    ) {

  test("should work with limit cancellation in nested apply") {
    val (nodes1, nodes2, rels1, rels2) = givenGraph {
      bidirectionalBipartiteGraph(5, "A", "B", "AB", "BA")
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("var3")
      .apply()
      .|.apply()
      .|.|.limit(1)
      .|.|.apply()
      .|.|.|.argument()
      .|.|.pruningVarExpand("(var3)<-[*1..1]-(var4)")
      .|.|.argument("var0", "var1", "var2", "var3")
      .|.allNodeScan("var3", "var0", "var1", "var2")
      .allRelationshipsScan("(var1)-[var0]->(var2)")
      .build()

    val runtimeResult = execute(query, runtime)

    val rels = rels1 ++ rels2
    val nodes = nodes1 ++ nodes2

    val expected = for {
      r <- rels
      n <- nodes
    } yield Array[Any](n)

    runtimeResult should beColumns("var3").withRows(expected)
  }

  test("should work with limit cancellation") {
    val (_, rels) = givenGraph {
      circleGraph(sizeHint, "R", 1)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("r")
      .semiApply()
      .|.limit(1)
      .|.pruningVarExpand("(c)<-[*1..1]-(d)")
      .|.allNodeScan("c")
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = execute(query, runtime)

    val expected = rels.map(Array(_))

    runtimeResult should beColumns("r").withRows(expected)
  }

  test("should expand into and provide variables for relationship - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .pruningVarExpand("(x)-[*1..1]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should use fallback correctly if rows are filtered out by fallback pipe") {
    // given
    val rels = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .pruningVarExpand("(n)-[*1..1]->(m)")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.map { r => Array(r.getStartNode, r.getEndNode) }
    runtimeResult should beColumns("n", "m").withRows(expected)
  }

  test("should use fallback correctly if output morsel has more slots than input morsel") {
    // given
    val rels = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("foo")
      .projection("n.prop AS foo")
      .pruningVarExpand("(n)-[*1..1]->(m)")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.map { _ => Array(null) }
    runtimeResult should beColumns("foo").withRows(expected)
  }

  test("should get exception with error plan") {
    givenGraph { nodeGraph(10) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .errorPlan(new HintException("hello"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    a[HintException] should be thrownBy {
      consume(runtimeResult)
    }
  }

  test("should do multiple middle correctly and profile") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r") // 0
      .nonFuseable()
      .nonPipelined() // 2
      .nonFuseable()
      .nonPipelined() // 4
      .nonFuseable()
      .input(relationships = Seq("r")) // 6
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(rels.map(Array[Any](_)): _*))

    // then
    val expected = rels.map(rel => Array(rel))

    runtimeResult should beColumns("r").withRows(expected)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    // ROWS
    queryProfile.operatorProfile(0).rows() shouldBe relTuples.size // produce result
    queryProfile.operatorProfile(2).rows() shouldBe relTuples.size // nonPipelined
    queryProfile.operatorProfile(4).rows() shouldBe relTuples.size // nonPipelined
    queryProfile.operatorProfile(6).rows() shouldBe relTuples.size // input

    // TIME
    queryProfile.operatorProfile(0).time() should be > 0L // produce result
    queryProfile.operatorProfile(2).time() should be > 0L // nonPipelined
    queryProfile.operatorProfile(4).time() should be > 0L // nonPipelined
    queryProfile.operatorProfile(6).time() should be > 0L // input
  }

  test("should profile head with multiple middle correctly") {
    // given
    val (nodes, rels) = givenGraph {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y") // 0
      .nonPipelined() // 1
      .nonPipelined() // 2
      .pruningVarExpand("(x)-[*1..1]->(y)") // 3
      .input(nodes = Seq("x"), relationships = Seq("r")) // 4
      .build()

    val runtimeResult =
      profile(logicalQuery, runtime, inputValues((0 until sizeHint).map(i => Array[Any](nodes(i), rels(i))): _*))
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    // ROWS
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce result
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // nonPipelined
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // nonPipelined
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint // var expand
    queryProfile.operatorProfile(4).rows() shouldBe sizeHint // input

    // TIME
    queryProfile.operatorProfile(0).time() should be > 0L // produce result
    queryProfile.operatorProfile(1).time() shouldBe OperatorProfile.NO_DATA // nonPipelined
    queryProfile.operatorProfile(2).time() shouldBe OperatorProfile.NO_DATA // nonPipelined
    queryProfile.operatorProfile(3).time() shouldBe OperatorProfile.NO_DATA // var expand
    queryProfile.operatorProfile(4).time() should be > 0L // input

    // DB HITS
    queryProfile.operatorProfile(0).dbHits() shouldBe sizeHint * 3 // produce result
    queryProfile.operatorProfile(1).dbHits() shouldBe sizeHint // nonPipelined
    queryProfile.operatorProfile(2).dbHits() shouldBe sizeHint // nonPipelined
    queryProfile.operatorProfile(3).dbHits() shouldBe 2 * sizeHint // var expand
    queryProfile.operatorProfile(4).dbHits() shouldBe 0L // input
  }
}
