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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType

abstract class UnionTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should union single variable") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.projection("1 AS x")
      .|.argument()
      .projection("'hi' AS x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(1, "hi")))
  }

  test("limit on top of union on rhs of apply") {
    val nodes = givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(1)
      .|.union()
      .|.|.argument("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
    runtimeResult.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe sizeHint // limit
    runtimeResult.runtimeResult.queryProfile().operatorProfile(3).rows().toInt should be >= sizeHint // union
  }

  test("limit 2 on top of union on rhs of apply") {
    val nodes = givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(2)
      .|.union()
      .|.|.argument("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes ++ nodes))

    // then
    runtimeResult.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe sizeHint * 2 // limit
    runtimeResult.runtimeResult.queryProfile().operatorProfile(3).rows() shouldBe sizeHint * 2 // union
  }

  test("limit 3 on top of union on rhs of apply") {
    val nodes = givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(3)
      .|.union()
      .|.|.argument("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes ++ nodes))

    // then
    runtimeResult.runtimeResult.queryProfile().operatorProfile(2).rows() shouldBe sizeHint * 2 // limit
    runtimeResult.runtimeResult.queryProfile().operatorProfile(3).rows() shouldBe sizeHint * 2 // union
  }

  test("should union single node variable") {
    // given
    val nodes = givenGraph { nodeGraph(Math.sqrt(sizeHint).toInt) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes ++ nodes))
  }

  test("should union after expands") {
    // given
    val nodes = givenGraph {
      val (nodes, _) = circleGraph(Math.sqrt(sizeHint).toInt)
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .sort("y ASC")
      .filter("id(y) >= 0")
      .union()
      .|.expand("(z)-->(y)")
      .|.allNodeScan("z")
      .expand("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(nodes ++ nodes))
  }

  test("should union node and non-node variable") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint / 2) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .union()
      .|.projection("'right' AS y")
      .|.allNodeScan("x")
      .projection("'left' AS y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(
      nodes.map(n => Array[Object](n, "left")) ++
        nodes.map(n => Array[Object](n, "right"))
    )
  }

  test("should union node and relationship variables") {
    // given
    val (_, rels) = givenGraph { circleGraph(sizeHint / 2) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r")
      .union()
      .|.expand("(x)-[r]->()")
      .|.allNodeScan("x")
      .expand("(x)<-[r]-()")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r").withRows(
      rels.map(r => Array(r.getStartNode, r)) ++
        rels.map(r => Array(r.getEndNode, r))
    )
  }

  test("should union nodes with other values") {
    val size = sizeHint / 2
    // given
    val nodes = givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.allNodeScan("x")
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(nodes.map(Array(_)) ++ input.flatten)
  }

  test("should union many variables") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .union()
      .|.projection("a+10 AS x", "a+20 AS y", "a+30 AS z")
      .|.unwind("[1,2,3,4,5,6,7,8,9] AS a")
      .|.argument()
      .projection("'hi' AS x", "'ho' AS y", "'humbug' AS z")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(
      (1 to 9).map(a => Array[Any](a + 10, a + 20, a + 30)) :+
        Array("hi", "ho", "humbug")
    )
  }

  test("should union many variables in permuted order") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .union()
      .|.projection("a+10 AS x", "a+20 AS y", "a+30 AS z")
      .|.unwind("[1,2,3,4,5,6,7,8,9] AS a")
      .|.argument()
      .projection("'ho' AS y", "'hi' AS x", "'humbug' AS z")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(
      (1 to 9).map(a => Array[Any](a + 10, a + 20, a + 30)) :+
        Array("hi", "ho", "humbug")
    )
  }

  test("should union cached properties") {
    val size = sizeHint / 2
    givenGraph { nodePropertyGraph(size, { case i => Map("prop" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[x.prop] AS prop")
      .union()
      .|.cacheProperties("cache[x.prop]")
      .|.allNodeScan("x")
      .cacheProperties("cache[x.prop]")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("prop").withRows(singleColumn((0 until size) ++ (0 until size)))
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 0 // projection
  }

  test("should union different cached properties from left and right") {
    val size = sizeHint / 2
    givenGraph {
      nodePropertyGraph(size, { case i => Map("foo" -> s"foo-$i", "bar" -> s"bar-$i") })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("foo", "bar")
      .projection("cache[x.foo] AS foo", "cache[x.bar] AS bar")
      .union()
      .|.cacheProperties("cache[x.bar]")
      .|.allNodeScan("x")
      .cacheProperties("cache[x.foo]")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    val expected = ((0 until size) ++ (0 until size)).map(i => Array(s"foo-$i", s"bar-$i"))

    // then
    runtimeResult should beColumns("foo", "bar").withRows(expected)
  }

  test("should unwind after union") {
    val size = sizeHint / 2
    // given
    val nodes = givenGraph {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "n")
      .unwind("[1, 2, 3, 4, 5] AS n")
      .union()
      .|.allNodeScan("x")
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for {
      x <- nodes ++ inputVals
      n <- Seq(1, 2, 3, 4, 5)
    } yield Array(x, n)

    runtimeResult should beColumns("x", "n").withRows(expected)
  }

  test("should distinct after union") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .distinct("res AS res")
      .union()
      .|.projection("y AS res")
      .|.unwind("[1, 2, 3, 4, 2, 5, 6, 7, 1] AS y")
      .|.argument()
      .projection("x AS res")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      res <- nodes ++ Seq(1, 2, 3, 4, 5, 6, 7)
    } yield Array(res)

    runtimeResult should beColumns("res").withRows(expected)
  }

  test("should work with limit on RHS") {
    val size = sizeHint / 2
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.limit(1)
      .|.allNodeScan("x")
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(size + 1))
  }

  test("should work with limit on LHS") {
    val size = sizeHint / 2
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.allNodeScan("x")
      .limit(1)
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(size + 1))
  }

  test("should work with limit on top") {
    val size = sizeHint / 2
    // given
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(1)
      .union()
      .|.allNodeScan("x")
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(1))
  }

  test("should work with limit under apply") {
    val size = sizeHint / 2
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.limit(1)
      .|.union()
      .|.|.allNodeScan("a")
      .|.argument()
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a").withRows(rowCount(size))
  }

  test("should work with distinct and limit under apply, filter on lhs") {
    val size = sizeHint / 2
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.limit(1)
      .|.distinct("a AS a")
      .|.union()
      .|.|.allNodeScan("a")
      .|.filter("id(a) % 10 >= 5") // With this filter the lowest argument ids through union will come only from RHS
      .|.argument()
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a").withRows(rowCount(size))
  }

  test("should work with distinct and limit under apply, filter on rhs") {
    val size = sizeHint / 2
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.limit(1)
      .|.distinct("a AS a")
      .|.union()
      .|.|.filter(
        "id(a) % 10 >= 5"
      ) // With this filter the lowest argument ids through union in will come only from LHS
      .|.|.allNodeScan("a")
      .|.argument()
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a").withRows(rowCount(size))
  }

  test("should union under apply") {
    val size = Math.sqrt(sizeHint).toInt
    // given
    val nodes = givenGraph {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res2")
      .apply()
      .|.projection("res AS res2")
      .|.union()
      .|.|.projection("x AS res")
      .|.|.argument("x")
      .|.projection("y AS res")
      .|.allNodeScan("y")
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for {
      x <- inputVals
      res <- x +: nodes
    } yield Array(res)

    runtimeResult should beColumns("res2").withRows(expected)
  }

  test("should union under apply with long slot aliases") {
    val size = Math.sqrt(sizeHint).toInt
    // given
    val nodes = givenGraph {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res2")
      .apply()
      .|.projection("res AS res2")
      .|.union()
      .|.|.projection("x AS res")
      .|.|.argument("x")
      .|.projection("x AS res")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      node <- nodes
      res2 <- Seq(node, node)
    } yield Array(res2)

    runtimeResult should beColumns("res2").withRows(expected)
  }

  test("should union under apply with follow-up operator") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .apply()
      .|.distinct("res AS res")
      .|.union()
      .|.|.projection("y AS res")
      .|.|.unwind("[1, 2, 3, 4, 2, 5, 6, 7, 1] AS y")
      .|.|.argument()
      .|.projection("x AS res")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- nodes
      res <- x +: Seq(1, 2, 3, 4, 5, 6, 7)
    } yield Array(res)

    runtimeResult should beColumns("res").withRows(expected)
  }

  test("should union under cartesian product with follow-up operator") {
    val size = 5 // Math.sqrt(sizeHint).toInt
    // given
    val nodes = givenGraph {
      nodeGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "res")
      .cartesianProduct()
      .|.distinct("res AS res")
      .|.union()
      .|.|.projection("y AS res")
      .|.|.unwind("[1, 2, 3, 4, 2, 5, 6, 7, 1] AS y")
      .|.|.argument()
      .|.projection("n AS res")
      .|.allNodeScan("n")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- nodes
      res <- nodes ++ Seq(1, 2, 3, 4, 5, 6, 7)
    } yield Array(x, res)

    runtimeResult should beColumns("x", "res").withRows(expected)
  }

  test("should union with alias on RHS") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "x")
      .union()
      .|.projection("x AS xxx")
      .|.projection("b AS a", "2 AS x")
      .|.allNodeScan("b")
      .projection("1 AS x")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array[Any](n, 1)) ++ nodes.map(n => Array[Any](n, 2))
    runtimeResult should beColumns("a", "x").withRows(expected)
  }

  test("should union with alias on LHS") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "x")
      .union()
      .|.projection("1 AS x")
      .|.allNodeScan("a")
      .projection("x AS xxx")
      .projection("b AS a", "2 AS x")
      .allNodeScan("b")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array[Any](n, 1)) ++ nodes.map(n => Array[Any](n, 2))
    runtimeResult should beColumns("a", "x").withRows(expected)
  }

  test("union with apply on RHS") {
    val size = sizeHint / 2
    // given
    val nodes = givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.apply()
      .|.|.projection("y AS x")
      .|.|.argument("y")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      node <- nodes
      x <- Seq(node, node)
    } yield Array(x)

    runtimeResult should beColumns("x").withRows(expected)
  }

  test("union with apply on LHS") {
    val size = sizeHint / 2
    // given
    val nodes = givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.allNodeScan("x")
      .apply()
      .|.projection("y AS x")
      .|.argument("y")
      .allNodeScan("y")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      node <- nodes
      x <- Seq(node, node)
    } yield Array(x)

    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should union on the RHS of a hash join") {
    val size = sizeHint / 3
    // given
    val (as, bs) = givenGraph {
      val as = nodeGraph(size, "A")
      val bs = nodeGraph(size, "B")
      nodeGraph(size, "C")
      (as, bs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.union()
      .|.|.nodeByLabelScan("x", "B", IndexOrderNone)
      .|.nodeByLabelScan("x", "A", IndexOrderNone)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- as ++ bs
    } yield Array(x)

    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should union with reducers") {
    val size = sizeHint / 3
    // given
    val (as, bs) = givenGraph {
      val as = nodeGraph(size, "A")
      val bs = nodeGraph(size, "B")
      nodeGraph(size, "C")
      (as, bs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .union()
      .|.sort("x ASC")
      .|.nodeByLabelScan("x", "B", IndexOrderNone)
      .sort("x ASC")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      x <- (as ++ bs).sortBy(_.getId)
    } yield Array(x)

    runtimeResult should beColumns("x").withRows(inOrder(expected))
  }

  test("should work with non-fused limit") {
    val size = sizeHint / 2
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .limit(1)
      .nonFuseable()
      .unwind("range (1, 10) AS y")
      .union()
      .|.allNodeScan("x")
      .input(variables = Seq("x"))
      .build()

    val inputVals = randomValues(size)
    val input = inputValues(inputVals.map(Array[Any](_)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y").withRows(Seq(Array[Any](1)))
  }

  test("union + unwind followed by apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHintAlignedToMorselSize)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "x")
      .apply()
      .|.argument("n", "x")
      .unwind("[n] AS x")
      .union()
      .|.nodeByLabelScan("n", "DoesNotExist")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "x").withRows(nodes.map(n => Array(n, n)))
  }

  test("union + unwind on RHS of apply, followed by apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHintAlignedToMorselSize)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "x")
      .apply()
      .|.argument("n", "x")
      .apply()
      .|.unwind("[n] AS x")
      .|.union()
      .|.|.nodeByLabelScan("n", "DoesNotExist")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "x").withRows(nodes.map(n => Array(n, n)))
  }

  test("union must not initialize RHS before LHS is exhausted") {
    assume(!isParallel, "Parallel does not yet support `Create`")

    val nodes = givenGraph {
      nodeGraph(sizeHintAlignedToMorselSize)
    }.size

    // This behavior is needed for MERGE to work correctly if present in both branches of the union.
    // The plan below is simplified.

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .union()
      .|.projection("m AS n")
      .|.nodeByLabelScan("m", "M")
      .create(createNode("m", "M"))
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "m").withRows(RowCount(2 * nodes))
  }

  test("union works between long and ref slots") {
    val nodeA = givenGraph {
      val nodeA = nodeGraph(1, "A").head
      val nodeB = nodeGraph(2, "B")
      nodeB.foreach(node => nodeA.createRelationshipTo(node, RelationshipType.withName("REL")))

      nodeA
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .optional()
      .expandAll("(n)-[rel]->(m)")
      .union()
      .|.nodeByLabelScan("n", "A")
      .projection("head(c) AS n")
      .aggregation(Seq(), Seq("collect(nB) AS c"))
      .nodeByLabelScan("nB", "B")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("n").withRows(singleColumn(Seq(nodeA, nodeA)))
  }

  test("should work with alias as argument") {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("bar")
      .apply()
      .|.optional("bar", "n")
      .|.union()
      .|.|.unwind("[] AS n")
      .|.|.argument()
      .|.unwind("[] AS n")
      .|.argument()
      .projection("foo AS bar")
      .projection("'foo' AS foo")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("bar").withRows(singleColumn(Seq("foo")))
  }

  private def sizeHintAlignedToMorselSize: Int = {
    val morselSize = edition.cypherConfig.pipelinedBatchSizeSmall
    (1 + sizeHint / morselSize) * morselSize
  }
}
