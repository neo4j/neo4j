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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.MergeConstraintConflictException
import org.neo4j.graphdb.schema.IndexType

abstract class AssertSameNodeTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should verify that two nodes are identical") {
    val nodes = given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should fail if two nodes are different") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Honey(prop = 21)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should verify that many nodes are identical") {
    val nodes = given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.assertSameNode("x")
      .|.|.assertSameNode("x")
      .|.|.|.nodeIndexOperator("x:Honey(prop = 20)")
      .|.|.nodeIndexOperator("x:Honey(prop = 20)")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should fail if two nodes out of many are different") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.assertSameNode("x")
      .|.|.assertSameNode("x")
      .|.|.|.nodeIndexOperator("x:Honey(prop = 20)")
      .|.|.nodeIndexOperator("x:Honey(prop = 21)")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should verify that three nodes are identical") {
    val nodes = given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.assertSameNode("x")
      .|.|.nodeIndexOperator("x:Honey(prop = 20)")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should fail if any of  that three nodes are different") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.assertSameNode("x")
      .|.|.nodeIndexOperator("x:Honey(prop = 21)")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should verify that two nodes are identical on the RHS of an apply") {
    val nodes = given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.assertSameNode("x")
      .|.|.nodeIndexOperator("x:Honey(prop = 20)")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should fail if two nodes are different on the RHS of an apply") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.assertSameNode("x")
      .|.|.nodeIndexOperator("x:Honey(prop = 20)")
      .|.nodeIndexOperator("x:Honey(prop = 21)")
      .argument()
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if only lhs is empty") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator(s"x:Honey(prop = ${sizeHint + 1})")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if only rhs is empty") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator(s"x:Honey(prop = ${sizeHint + 1})")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should work if lhs and rhs are empty") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator(s"x:Honey(prop = ${sizeHint + 1})")
      .nodeIndexOperator(s"x:Honey(prop = ${sizeHint + 1})")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should assert same nodes on top of range seek and fail") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})")
      .nodeIndexOperator(s"x:Honey(prop > ${sizeHint / 2})")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail on merge using multiple unique indexes if it found a node matching single property only") {
    given {
      uniqueNodeIndex("Person", "id")
      uniqueNodeIndex("Person", "email")
      nodePropertyGraph(
        1,
        {
          case _ => Map("email" -> "smth@neo.com")
        },
        "Person"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Person(email = 'smth@neo.com')", unique = true)
      .nodeIndexOperator("x:Person(id = 42)", unique = true)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should fail on merge using multiple unique indexes if it found a node matching single property only, flipped order"
  ) {
    given {
      uniqueNodeIndex("Person", "id")
      uniqueNodeIndex("Person", "email")
      nodePropertyGraph(
        1,
        {
          case _ => Map("id" -> 42)
        },
        "Person"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Person(email = 'smth@neo.com')", unique = true)
      .nodeIndexOperator("x:Person(id = 42)", unique = true)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("two unique indexes same node") {
    val nodes = given {
      uniqueNodeIndex("L", "prop1")
      uniqueNodeIndex("L", "prop2")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i.toString)
        },
        "L"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:L(prop2 = '20')")
      .nodeIndexOperator("x:L(prop1 = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("two unique indexes different nodes") {
    given {
      uniqueNodeIndex("L", "prop1")
      uniqueNodeIndex("L", "prop2")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i.toString)
        },
        "L"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:L(prop2 = '21')")
      .nodeIndexOperator("x:L(prop1 = 20)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }
}

trait EnterpriseAssertSameNodeTestBase[CONTEXT <: RuntimeContext] {
  self: AssertSameNodeTestBase[CONTEXT] =>

  test("one node key constraint, same node") {
    val nodes = given {
      nodeKey("L", "prop1", "prop2")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i.toString)
        },
        "L"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:L(prop1 = 20, prop2 = '20')", unique = true, indexType = IndexType.RANGE)
      .nodeIndexOperator("x:L(prop1 = 20, prop2 = '20')", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("one node key constraint, different nodes") {
    given {
      nodeKey("L", "prop1", "prop2")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i.toString)
        },
        "L"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:L(prop1 = 21, prop2 = '21')", unique = true, indexType = IndexType.RANGE)
      .nodeIndexOperator("x:L(prop1 = 20, prop2 = 20)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("two node key constraints, same node") {
    val nodes = given {
      nodeKey("L", "prop1", "prop2")
      nodeKey("L", "prop3", "prop4")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i.toString, "prop3" -> i, "prop4" -> i.toString)
        },
        "L"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:L(prop3 = 20, prop4 = '20')", unique = true, indexType = IndexType.RANGE)
      .nodeIndexOperator("x:L(prop1 = 20, prop2 = '20')", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("two node key constraints, different nodes") {
    given {
      nodeKey("L", "prop1", "prop2")
      nodeKey("L", "prop3", "prop4")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> i.toString, "prop3" -> i, "prop4" -> i.toString)
        },
        "L"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:L(prop3 = 21, prop4 = '21')", unique = true, indexType = IndexType.RANGE)
      .nodeIndexOperator("x:L(prop1 = 20, prop2 = 20)", unique = true, indexType = IndexType.RANGE)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

}

/**
 * These are tests for cases which don't really happen in production
 * but is still supported by the legacy implementation.
 */
trait EsotericAssertSameNodeTestBase[CONTEXT <: RuntimeContext] {
  self: AssertSameNodeTestBase[CONTEXT] =>

  test("should fail if lhs is not a node") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .projection("y.prop AS x")
      .nodeIndexOperator("y:Honey(prop = 20)")
      .build()

    // then
    a[CypherTypeException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if rhs is not a node") {
    given {
      uniqueNodeIndex("Honey", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.projection("y.prop AS x")
      .|.nodeIndexOperator("y:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    // then
    a[CypherTypeException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should handle more nodes on the lhs") {
    val (nodes, _) = given {
      uniqueNodeIndex("Honey", "prop")
      bipartiteGraph(
        sizeHint,
        "Honey",
        "Bee",
        "R",
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.nodeIndexOperator("x:Honey(prop = 2)")
      .expand("(x)--(y)")
      .nodeIndexOperator("x:Honey(prop = 2)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(2)
    runtimeResult should beColumns("x").withRows(singleColumn((1 to sizeHint).map(_ => expected)))
  }

  test("should handle more nodes on the rhs") {
    val (nodes, _) = given {
      uniqueNodeIndex("Honey", "prop")
      bipartiteGraph(
        sizeHint,
        "Honey",
        "Bee",
        "R",
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.expand("(x)--(y)")
      .|.nodeIndexOperator("x:Honey(prop = 20)")
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected)
  }

  test("should fail if some of the nodes coming from rhs are different") {
    given {
      uniqueNodeIndex("Honey", "prop")
      bipartiteGraph(
        sizeHint,
        "Honey",
        "Bee",
        "R",
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .assertSameNode("x")
      .|.expand("(x)--(y)")
      .|.nodeByLabelScan("x", "Honey", IndexOrderNone)
      .nodeIndexOperator("x:Honey(prop = 20)")
      .build()

    execute(logicalQuery, runtime)

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

}
