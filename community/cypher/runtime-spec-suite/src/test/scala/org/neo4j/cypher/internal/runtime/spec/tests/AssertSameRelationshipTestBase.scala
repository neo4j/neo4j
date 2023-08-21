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
import org.neo4j.exceptions.MergeConstraintConflictException
import org.neo4j.graphdb.RelationshipType

abstract class AssertSameRelationshipTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should verify that two relationships are identical (directed)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withSingleRow(expected)
  }

  test("should verify that two relationships are identical (undirected)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected)))
  }

  test("should fail if two relationships are different (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 21)]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if two relationships are different (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 21)]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should verify that many relationships are identical (directed)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.assertSameRelationship("r")
      .|.|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withSingleRow(expected)
  }

  test("should verify that many relationships are identical (undirected)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.assertSameRelationship("r")
      .|.|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected)))
  }

  test("should fail if two relationships out of many are different (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.assertSameRelationship("r")
      .|.|.|.relationshipIndexOperator("(x)-[r:R(prop = 21)]->(y)")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if two relationships out of many are different (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.assertSameRelationship("r")
      .|.|.|.relationshipIndexOperator("(x)-[r:R(prop = 21)]-(y)")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should verify that three relationships are identical (directed)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withSingleRow(expected)
  }

  test("should verify that three relationships are identical (undirected)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected)))
  }

  test("should fail if any of that three nodes are different (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 21)]->(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if any of that three nodes are different (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 21)]-(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should verify that two relationships are identical on the RHS of an apply (directed)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withSingleRow(expected)
  }

  test("should verify that two relationships are identical on the RHS of an apply (undirected)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected)))
  }

  test("should fail if two relationships are different on the RHS of an apply (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 21)]->(y)")
      .argument()
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if two relationships are different on the RHS of an apply (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 21)]-(y)")
      .argument()
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if only lhs is empty (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]->(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if only lhs is empty (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]-(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if only rhs is empty (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]->(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should fail if only rhs is empty (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop = 20)]-(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should work if lhs and rhs are empty (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]->(y)")
      .relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withNoRows()
  }

  test("should work if lhs and rhs are empty (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]-(y)")
      .relationshipIndexOperator(s"(x)-[r:R(prop = ${sizeHint + 1})]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r").withNoRows()
  }

  test("should assert same relationships on top of range seek and fail (directed)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]->(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("should assert same relationships on top of range seek and fail (undirected)") {
    given {
      uniqueRelationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)")
      .relationshipIndexOperator(s"(x)-[r:R(prop > ${sizeHint / 2})]-(y)")
      .build()

    // then
    a[MergeConstraintConflictException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test(
    "should fail on merge using multiple unique indexes if it found a relationship matching single property only (directed)"
  ) {
    given {
      uniqueRelationshipIndex("R", "id")
      uniqueRelationshipIndex("R", "email")
      tx.createNode()
        .createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
        .setProperty("email", "smth@neo.com")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(email = 'smth@neo.com')]->(y)", unique = true)
      .relationshipIndexOperator("(x)-[r:R(id = 42)]->(y)", unique = true)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should fail on merge using multiple unique indexes if it found a relationship matching single property only (undirected)"
  ) {
    given {
      uniqueRelationshipIndex("R", "id")
      uniqueRelationshipIndex("R", "email")
      tx.createNode()
        .createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
        .setProperty("email", "smth@neo.com")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(email = 'smth@neo.com')]-(y)", unique = true)
      .relationshipIndexOperator("(x)-[r:R(id = 42)]-(y)", unique = true)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should fail on merge using multiple unique indexes if it found a relationship matching single property only, flipped order (directed)"
  ) {
    given {
      uniqueRelationshipIndex("R", "id")
      uniqueRelationshipIndex("R", "email")
      tx.createNode()
        .createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
        .setProperty("id", 42)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(email = 'smth@neo.com')]->(y)", unique = true)
      .relationshipIndexOperator("(x)-[r:R(id = 42)]->(y)", unique = true)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test(
    "should fail on merge using multiple unique indexes if it found a relationship matching single property only, flipped order (undirected)"
  ) {
    given {
      uniqueRelationshipIndex("R", "id")
      uniqueRelationshipIndex("R", "email")
      tx.createNode()
        .createRelationshipTo(tx.createNode(), RelationshipType.withName("R"))
        .setProperty("id", 42)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(email = 'smth@neo.com')]-(y)", unique = true)
      .relationshipIndexOperator("(x)-[r:R(id = 42)]-(y)", unique = true)
      .build(readOnly = false)

    // then
    a[MergeConstraintConflictException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("two unique indexes same relationships (directed)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop1")
      uniqueRelationshipIndex("R", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("prop1", i)
          r.setProperty("prop2", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop2 = '20')]->(y)")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 20)]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withSingleRow(expected)
  }

  test("two unique indexes same relationships (undirected)") {
    val rels = given {
      uniqueRelationshipIndex("R", "prop1")
      uniqueRelationshipIndex("R", "prop2")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("prop1", i)
          r.setProperty("prop2", i.toString)
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .assertSameRelationship("r")
      .|.relationshipIndexOperator("(x)-[r:R(prop2 = '20')]-(y)")
      .relationshipIndexOperator("(x)-[r:R(prop1 = 20)]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = rels(20)
    runtimeResult should beColumns("r").withRows(singleColumn(Seq(expected, expected)))
  }
}
