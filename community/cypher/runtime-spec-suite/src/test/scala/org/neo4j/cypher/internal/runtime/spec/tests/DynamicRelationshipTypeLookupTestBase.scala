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
import org.neo4j.cypher.internal.expressions.NullCheckAssert
import org.neo4j.cypher.internal.expressions.NullCheckAssert.NullCheckAssertException
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipWithDynamicType
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicElement.All
import org.neo4j.cypher.internal.logical.plans.DynamicElement.Any
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.notification.RuntimeUnsatisfiableRelationshipTypeExpression
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException

object DynamicRelationshipTypeLookupTestBase

abstract class DynamicRelationshipTypeLookupTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  def outgoing(rel: Relationship): Array[Entity] = Array(rel, rel.getStartNode, rel.getEndNode)

  def incoming(rel: Relationship): Array[Entity] = Array(rel, rel.getEndNode, rel.getStartNode)

  def expectDirected(rels: Seq[Relationship]): Seq[Array[Entity]] = rels.map(outgoing)

  def expectUndirected(rels: Seq[Relationship]): Seq[Array[Entity]] =
    rels.flatMap(rel => Seq(outgoing(rel), incoming(rel)))

  test("directed scan") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (_, _, rels, _) = givenGraph {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectDirected(rels))
  }

  test("directed scan (incoming)") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (_, _, rels, _) = givenGraph {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)<-[r]-(y)", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(rels.map(incoming))
  }

  test("directed scan + filter") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    givenGraph {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("multiple directed scans") {
    // given
    val (_, relationships) = givenGraph { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.dynamicRelationshipTypeLookup("()-[r3]->()", "$('R')")
      .apply()
      .|.dynamicRelationshipTypeLookup("()-[r2]->()", "$('R')")
      .dynamicRelationshipTypeLookup("()-[r1]->()", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { r1 <- relationships; r2 <- relationships; r3 <- relationships } yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("directed scan with an argument") {
    // given
    val (_, relationships) = givenGraph { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "r")
      .apply()
      .|.projection("a AS b")
      .|.dynamicRelationshipTypeLookup("()-[r]->()", "$('R')", argumentIds = Set("a"))
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(3)))
    val expected = for (i <- 1 to 3; r <- relationships) yield Array[Any](i, r)
    runtimeResult should beColumns("b", "r").withRows(expected)
  }

  test("undirected scan") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (_, _, relationships, _) = givenGraph {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectUndirected(relationships))
  }

  test("undirected scan + filter") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    givenGraph {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("multiple undirected scans") {
    // given
    val (_, relationships) = givenGraph { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.dynamicRelationshipTypeLookup("()-[r3]-()", "$('R')")
      .apply()
      .|.dynamicRelationshipTypeLookup("()-[r2]-()", "$('R')")
      .dynamicRelationshipTypeLookup("()-[r1]-()", "$('R')")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- relationships; r2 <- relationships; r3 <- relationships
    } yield Seq.fill(8)(Array(r1, r2, r3))
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected.flatten)
  }

  test("undirected scan with an argument") {
    // given
    val (_, relationships) = givenGraph { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "r")
      .apply()
      .|.projection("a AS b")
      .|.dynamicRelationshipTypeLookup("()-[r]-()", "$('R')", argumentIds = Set("a"))
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(3)))
    val expected = (for (i <- 1 to 3; r <- relationships) yield Seq(Array[Any](i, r), Array[Any](i, r))).flatten
    runtimeResult should beColumns("b", "r").withRows(expected)
  }

  test("undirected scan with a continuation") {
    val size = 100
    val (_, rels) = givenGraph {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .dynamicRelationshipTypeLookup("(n)-[r]-(m)", "$('R')")
      .build()

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(2 * 10)(r))))
  }

  test("undirected scans for all given types only find loop once") {
    val rel = givenGraph {
      val a = tx.createNode()
      a.createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$all(['R','R'])")
      .build()

    execute(logicalQuery, runtime) should beColumns("r", "x", "y").withSingleRow(outgoing(rel): _*)
  }

  test("undirected scans for any given types only find loop once") {
    val rel = givenGraph {
      val a = tx.createNode()
      a.createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$any(['R','S','T'])")
      .build()

    execute(logicalQuery, runtime) should beColumns("r", "x", "y").withSingleRow(outgoing(rel): _*)
  }

  test("directed scan for any types") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      val (_, _) = circleGraph(sizeHint / 3, "D", 1)
      val (_, _) = circleGraph(sizeHint / 3, "E", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any(['A','B','C'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectDirected(rels))
  }

  test("undirected scan for any types") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      val (_, _) = circleGraph(sizeHint / 3, "D", 1)
      val (_, _) = circleGraph(sizeHint / 3, "E", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$any(['A','B','C'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectUndirected(rels))
  }

  test("directed scan for any types derived from parameters") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      val (_, _) = circleGraph(sizeHint / 3, "D", 1)
      val (_, _) = circleGraph(sizeHint / 3, "E", 1)
      aRels ++ bRels ++ cRels
    }

    val inputStream = inputValues(Array("A"), Array(Array("B", "C")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any(relType)", argumentIds = Set("relType"))
      .input(variables = Seq("relType"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputStream)

    // then
    runtimeResult should beColumns("r").withRows(singleColumn(rels))
  }

  test("undirected scan for any types derived from parameters") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      val (_, _) = circleGraph(sizeHint / 3, "D", 1)
      val (_, _) = circleGraph(sizeHint / 3, "E", 1)
      aRels ++ bRels ++ cRels
    }

    val inputStream = inputValues(Array("A"), Array(Array("B", "C")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$any(relType)", argumentIds = Set("relType"))
      .input(variables = Seq("relType"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputStream)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectUndirected(rels))
  }

  test("multiple directed scans for any types") {
    // given
    val (_, rels) = givenGraph { circleGraph(10, "A", 1) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x3)-[r3]->(y3)", "$any(['A','B','C'])")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x2)-[r2]->(y2)", "$any(['A','B','C'])")
      .dynamicRelationshipTypeLookup("(x1)-[r1]->(y1)", "$any(['A','B','C'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { r1 <- rels; r2 <- rels; r3 <- rels } yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("multiple undirected scans for any types") {
    // given
    val (_, rels) = givenGraph { circleGraph(10, "A", 1) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x3)-[r3]-(y3)", "$any(['A','B','C'])")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x2)-[r2]-(y2)", "$any(['A','B','C'])")
      .dynamicRelationshipTypeLookup("(x1)-[r1]-(y1)", "$any(['A','B','C'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- rels.flatMap(r => Seq(r, r)); r2 <- rels.flatMap(r => Seq(r, r)); r3 <- rels.flatMap(r => Seq(r, r))
    } yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("should handle non-existing types, directed") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any(['A','B','C'])")
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("r", "x", "y").withNoRows()

    // CREATE A
    val (_, as) = givenGraph(circleGraph(sizeHint, "A", 1))
    val directedAs = expectDirected(as)
    execute(executablePlan) should beColumns("r", "x", "y").withRows(directedAs)

    // CREATE B
    val (_, bs) = givenGraph(circleGraph(sizeHint, "B", 1))
    val directedBs = expectDirected(bs)
    execute(executablePlan) should beColumns("r", "x", "y").withRows(directedAs ++ directedBs)

    // CREATE C
    val (_, cs) = givenGraph(circleGraph(sizeHint, "C", 1))
    val directedCs = expectDirected(cs)
    execute(executablePlan) should beColumns("r", "x", "y").withRows(directedAs ++ directedBs ++ directedCs)
  }

  test("should handle non-existing types, undirected") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$any(['A','B','C'])")
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("r", "x", "y").withNoRows()

    // CREATE A
    val (_, as) = givenGraph(circleGraph(sizeHint, "A", 1))
    val directedAs = expectUndirected(as)
    execute(executablePlan) should beColumns("r", "x", "y").withRows(directedAs)

    // CREATE B
    val (_, bs) = givenGraph(circleGraph(sizeHint, "B", 1))
    val directedBs = expectUndirected(bs)
    execute(executablePlan) should beColumns("r", "x", "y").withRows(directedAs ++ directedBs)

    // CREATE C
    val (_, cs) = givenGraph(circleGraph(sizeHint, "C", 1))
    val directedCs = expectUndirected(cs)
    execute(executablePlan) should beColumns("r", "x", "y").withRows(directedAs ++ directedBs ++ directedCs)
  }

  test("directed scan for any types on the RHS of an apply") {
    // given
    val (aRels, bRels, cRels, dRels) = givenGraph {
      val (_, aRels) = circleGraph(10, "A", 1)
      val (_, bRels) = circleGraph(10, "B", 1)
      val (_, cRels) = circleGraph(10, "C", 1)
      val (_, dRels) = circleGraph(10, "C", 1)

      (aRels, bRels, cRels, dRels)
    }

    val columns = Seq("r1", "x1", "y1", "r2", "x2", "y2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns: _*)
      .apply()
      .|.dynamicRelationshipTypeLookup("(x2)-[r2]->(y2)", "$any(['C','D'])", argumentIds = Set("x1", "r1", "y1"))
      .dynamicRelationshipTypeLookup("(x1)-[r1]->(y1)", "$any(['A','B'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- expectDirected(aRels) ++ expectDirected(bRels);
      r2 <- expectDirected(cRels) ++ expectDirected(dRels)
    } yield Array(r1, r2).flatten
    runtimeResult should beColumns(columns: _*).withRows(expected)
  }

  test("undirected scan for any types on the RHS of an apply") {
    // given
    val (aRels, bRels, cRels, dRels) = givenGraph {
      val (_, aRels) = circleGraph(10, "A", 1)
      val (_, bRels) = circleGraph(10, "B", 1)
      val (_, cRels) = circleGraph(10, "C", 1)
      val (_, dRels) = circleGraph(10, "C", 1)

      (aRels, bRels, cRels, dRels)
    }

    val columns = Seq("r1", "x1", "y1", "r2", "x2", "y2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(columns: _*)
      .apply()
      .|.dynamicRelationshipTypeLookup("(x2)-[r2]-(y2)", "$any(['C','D'])", argumentIds = Set("x1", "r1", "y1"))
      .dynamicRelationshipTypeLookup("(x1)-[r1]-(y1)", "$any(['A','B'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- expectUndirected(aRels) ++ expectUndirected(bRels)
      r2 <- expectUndirected(cRels) ++ expectUndirected(dRels)
    } yield Array(r1, r2).flatten
    runtimeResult should beColumns(columns: _*).withRows(expected)
  }

  test("scan should get source, target and type") {
    // given
    val rels = givenGraph {
      val (_, aRels) = circleGraph(sizeHint / 3, "A", 1)
      val (_, bRels) = circleGraph(sizeHint / 3, "B", 1)
      val (_, cRels) = circleGraph(sizeHint / 3, "C", 1)
      aRels ++ bRels ++ cRels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "t")
      .projection("x AS x", "y AS y", "type(r) AS t")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any(['A','B','C'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels.map(r => Array[Any](r.getStartNode, r.getEndNode, r.getType.name()))
    runtimeResult should beColumns("x", "y", "t").withRows(expected)
  }

  test("work with merge and a nonexistent rel type") {
    assume(!isParallel)
    assume(!isPipelined || canFuse)

    givenGraph {
      circleGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .merge(
        Seq(createNodeFull("a"), createNodeFull("b")),
        Seq(createRelationshipWithDynamicType("r", "a", "'Foo'", "b", OUTGOING))
      )
      .dynamicRelationshipTypeLookup("(a)-[r]->(b)", "$all('Foo')")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns().withStatistics(nodesCreated = 2, relationshipsCreated = 1)
  }

  test("work with merge and a nonexistent rel type and a property") {
    assume(canMerge)

    givenGraph {
      circleGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .merge(
        Seq(createNodeFull("a"), createNodeFull("b")),
        Seq(createRelationshipWithDynamicType("r", "a", "'Foo'", "b", OUTGOING, Some("{prop: 1}")))
      )
      .dynamicRelationshipTypeLookup("(a)-[r]->(b)", "$any('Foo')", propertyPredicates = Map("prop" -> "1"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns().withStatistics(nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1)
  }

  test("directed scan for any types from an empty array") {
    /* equivalent to the empty result set */
    givenGraph {
      circleGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any([])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withNoRows()
  }

  test("directed scan for all types from an empty array") {
    /* equivalent to the super set of all relationships */
    val (_, rels) = givenGraph {
      circleGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$all([])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectDirected(rels))
  }

  test("directed scan for any of the partially non-existent given types") {
    val (_, rels) = givenGraph {
      circleGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any(['NONEXISTENT_LABEL_1', 'R'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(expectDirected(rels))
  }

  test("directed scan for all of the partially non-existent given types") {
    givenGraph {
      circleGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$all(['NONEXISTENT_LABEL_1', 'R'])")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("fail with a helpful error message when the input is not a single string (all)") {
    // given
    givenGraph {
      val a = tx.createNode()
      tx.createNode().createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.dynamicRelationshipTypeLookup("()-[r]->()", "$(relType)", argumentIds = Set("relType"))
      .input(variables = Seq("relType"))
      .build()

    def theResultFor(v: Any) = execute(logicalQuery, runtime, inputValues(Array(v)))
    def theDynamicType(v: Any): Unit = consume(theResultFor(v))

    // then
    theResultFor(Array[String]("C", "C")) should beColumns("r").withNoNotifications()
    theResultFor(Array[String]("C", "D")) should beColumns("r").withNotifications(
      RuntimeUnsatisfiableRelationshipTypeExpression(List("C", "D"))
    )
    the[CypherTypeException] thrownBy theDynamicType(
      1
    ) should have message "Expected relationship type to be a string or list of strings."
    the[CypherTypeException] thrownBy theDynamicType(
      Array(1)
    ) should have message "Expected relationship type to be a string or list of strings."
    the[CypherTypeException] thrownBy theDynamicType(
      null
    ) should have message "Expected relationship type to be a string or list of strings."
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(""))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType("\u0000"))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("\u0000", "\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("", "\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("C", "")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("X", "")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("", "C")))
  }

  test("fail with a helpful error message when the input is not a string or list of strings (any)") {
    // given
    givenGraph {
      val a = tx.createNode()
      tx.createNode().createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.dynamicRelationshipTypeLookup("()-[r]->()", "$any(relType)", argumentIds = Set("relType"))
      .input(variables = Seq("relType"))
      .build()

    def theResultFor(v: Any) = execute(logicalQuery, runtime, inputValues(Array(v)))
    def theDynamicType(v: Any): Unit = consume(theResultFor(v))

    // then
    theResultFor(Array[String]("C", "C")) should beColumns("r").withNoNotifications()
    theResultFor(Array[String]("C", "D")) should beColumns("r").withNoNotifications()
    the[CypherTypeException] thrownBy theDynamicType(
      1
    ) should have message "Expected relationship type to be a string or list of strings."
    the[CypherTypeException] thrownBy theDynamicType(
      Array(1)
    ) should have message "Expected relationship type to be a string or list of strings."
    the[CypherTypeException] thrownBy theDynamicType(
      null
    ) should have message "Expected relationship type to be a string or list of strings."
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(""))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType("\u0000"))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("\u0000", "\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("", "\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("C", "")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("X", "")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("", "C")))
  }

  test("when conflicting types are encountered the next row should be processed") {
    // given
    val rel = givenGraph {
      val a = tx.createNode()
      tx.createNode().createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$all(relType)", argumentIds = Set("relType"))
      .input(variables = Seq("relType"))
      .build()

    val res = execute(
      logicalQuery,
      runtime,
      inputValues(
        Array(Array[String]("A", "B")),
        Array("R")
      )
    )

    res should beColumns("r", "x", "y")
      .withRows(expectDirected(Seq(rel)))
      .withNotifications(RuntimeUnsatisfiableRelationshipTypeExpression(List("A", "B")))
  }

  test("check for null input in dynamic type expression") {
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
    }

    Seq(SemanticDirection.BOTH, SemanticDirection.OUTGOING).foreach { dir =>
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .dynamicRelationshipTypeLookup(
          Some("x"),
          Some("r"),
          NullCheckAssert(),
          Some("y"),
          dir,
          DynamicElement.Any,
          IndexOrderNone,
          Map.empty,
          Set.empty
        )
        .build()

      the[Exception] thrownBy consume(execute(logicalQuery, runtime)) should not be a[NullCheckAssertException]
    }
  }

  // TYPE + PROPERTY SEEKS

  test("directed seek for a single type and single property") {
    val expected = givenGraph {
      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]->(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)
      runtimeResult should beColumns("r", "x", "y").withRows(expectDirected(expected))
    }
  }

  test("undirected seek for a single type and single property") {
    val expected = givenGraph {
      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]-(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1")
        )
        .build()

      val runtimeResult = execute(logicalQuery, runtime)
      runtimeResult should beColumns("r", "x", "y").withRows(expectUndirected(expected))
    }
  }

  test("directed seek for an empty type list and single property") {
    val expected = givenGraph {
      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode(Label.label("N"))
      n.setProperty("types", Array[String]())
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$(x.types)"
        case Any => "$any(x.types)"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .apply()
        .|.dynamicRelationshipTypeLookup("(x)-[r]->(y)", dynamicType, propertyPredicates = Map("prop" -> "1"))
        .nodeByLabelScan("x", "N")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      operator match {
        case All =>
          runtimeResult should beColumns("r", "x", "y").withRows(expectDirected(expected))
        case Any =>
          runtimeResult should beColumns("r", "x", "y").withNoRows()
      }
    }
  }

  test("undirected seek for an empty type list and single property") {
    val expected = givenGraph {
      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode(Label.label("N"))
      n.setProperty("types", Array[String]())
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$(x.types)"
        case Any => "$any(x.types)"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .apply()
        .|.dynamicRelationshipTypeLookup("(x)-[r]-(y)", dynamicType, propertyPredicates = Map("prop" -> "1"))
        .nodeByLabelScan("x", "N")
        .build()

      val runtimeResult = execute(logicalQuery, runtime)

      operator match {
        case All =>
          runtimeResult should beColumns("r", "x", "y").withRows(expectUndirected(expected))
        case Any =>
          runtimeResult should beColumns("r", "x", "y").withNoRows()
      }
    }
  }

  test("directed seek for a single type and single property using an index") {
    val indexName = "R_on_prop"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]->(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1")
        )
        .build()

      profile(
        logicalQuery,
        runtime
      ) should beColumns("r", "x", "y").withRows(expectDirected(expected)).usingIndexes(2, indexName)
    }
  }

  test("undirected seek for a single type and single property using an index") {
    val indexName = "R_on_prop"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]-(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1")
        )
        .build()

      profile(logicalQuery, runtime) should beColumns("r", "x", "y").withRows(expectUndirected(expected))
        .usingIndexes(2, indexName)
    }
  }

  test("directed seek for a single type and multiple properties using an index and filter") {
    val indexName = "R_on_prop"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)
      r.setProperty("prop2", 2)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")).setProperty("prop", 1)

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]->(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1", "prop2" -> "2")
        )
        .build()

      profile(logicalQuery, runtime) should beColumns("r", "x", "y").withRows(expectDirected(expected))
        .usingIndexes(2, indexName)
    }
  }

  test("undirected seek for a single type and multiple properties using an index and filter") {
    val indexName = "R_on_prop"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)
      r.setProperty("prop2", 2)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")).setProperty("prop", 1)

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]-(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1", "prop2" -> "2")
        )
        .build()

      profile(logicalQuery, runtime) should
        beColumns("r", "x", "y").withRows(expectUndirected(expected))
          .usingIndexes(2, indexName)
    }
  }

  test("directed seek for a single type and multiple properties using a compound index") {
    val indexName = "R_on_prop_and_prop2"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").on("prop2").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)
      r.setProperty("prop2", 2)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")).setProperty("prop2", 1)

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]->(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1", "prop2" -> "2")
        )
        .build()

      profile(logicalQuery, runtime) should
        beColumns("r", "x", "y").withRows(expectDirected(expected))
          .usingIndexes(2, indexName)
    }
  }

  test("undirected seek for a single type and multiple properties using a compound index") {
    val indexName = "R_on_prop_and_prop2"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").on("prop2").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)
      r.setProperty("prop2", 2)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")).setProperty("prop2", 1)

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]-(y)",
          dynamicType,
          propertyPredicates = Map("prop" -> "1", "prop2" -> "2")
        )
        .build()

      profile(logicalQuery, runtime) should
        beColumns("r", "x", "y").withRows(expectUndirected(expected))
          .usingIndexes(2, indexName)
    }
  }

  test("should pass property predicates to the kernel seek in the same order as they are defined in the index") {
    val indexName = "R_on_prop_and_prop2"
    val expected = givenGraph {
      relationshipIndex("R")(_.on("prop").on("prop2").withName(indexName))

      val relType = RelationshipType.withName("R")

      // MATCHED
      val n = tx.createNode()
      val r = n.createRelationshipTo(tx.createNode(), relType)
      r.setProperty("prop", 1)
      r.setProperty("prop2", 2)

      // NOT MATCHED
      tx.createNode().createRelationshipTo(tx.createNode(), relType)
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S"))
      tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")).setProperty("prop2", 1)

      Seq(r)
    }

    Seq(All, Any).foreach { operator =>
      val dynamicType = operator match {
        case All => "$('R')"
        case Any => "$any('R')"
      }

      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r", "x", "y")
        .filter("true")
        .dynamicRelationshipTypeLookup(
          "(x)-[r]-(y)",
          dynamicType,
          propertyPredicates = Map("prop2" -> "2", "prop" -> "1")
        )
        .build()

      profile(logicalQuery, runtime) should
        beColumns("r", "x", "y").withRows(expectUndirected(expected))
          .usingIndexes(2, indexName)
    }
  }
}
