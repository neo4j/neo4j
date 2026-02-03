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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.plans.DynamicElement.All
import org.neo4j.cypher.internal.logical.plans.DynamicElement.Any
import org.neo4j.cypher.internal.plandescription.Arguments.UsedIndexes
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException

import scala.collection.immutable.ListMap

object DynamicLabelNodeLookupTestBase

abstract class DynamicLabelNodeLookupTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should throw error on invalid label name") {

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.filter("true")
      .|.dynamicLabelNodeLookup("x", "relType", All, "relType")
      .input(variables = Seq("relType"))
      .build()

    def theResultFor(v: Any) = execute(logicalQuery, runtime, inputValues(Array(v)))
    def theDynamicType(v: Any): Unit = consume(theResultFor(v))

    // then
    the[CypherTypeException] thrownBy theDynamicType(
      1
    ) should have message "Expected node label to be a string or list of strings."
    the[CypherTypeException] thrownBy theDynamicType(
      Array(1)
    ) should have message "Expected node label to be a string or list of strings."
    the[CypherTypeException] thrownBy theDynamicType(
      null
    ) should have message "Expected node label to be a string or list of strings."
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(""))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType("\u0000"))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("\u0000", "\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("", "\u0000")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("C", "")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("X", "")))
    an[IllegalTokenNameException] shouldBe thrownBy(theDynamicType(Array("", "C")))
  }

  test("should scan all nodes of a dynamic label") {
    // given
    val honeys = givenGraph {
      nodeGraph(sizeHint, "Butter")
      nodeGraph(sizeHint, "Almond")

      nodeGraph(sizeHint, "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'Honey'", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(honeys))
  }

  test("should scan empty graph") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'Honey'", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle multiple scans") {
    // given
    val nodes = givenGraph { nodeGraph(10, "Honey") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z", "x")
      .apply()
      .|.filter("true")
      .|.dynamicLabelNodeLookup("x", "'Honey'", All)
      .apply()
      .|.filter("true")
      .|.dynamicLabelNodeLookup("y", "'Honey'", All)
      .dynamicLabelNodeLookup("z", "'Honey'", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { x <- nodes; y <- nodes; z <- nodes } yield Array(y, z, x)
    runtimeResult should beColumns("y", "z", "x").withRows(expected)
  }

  test("should union any labels of all nodes") {
    // given
    val expected = givenGraph {
      // not matched
      nodeGraph(sizeHint, "Honey")

      // matched
      nodeGraph(sizeHint, "Butter", "Almond") ++
        nodeGraph(sizeHint, "Butter") ++
        nodeGraph(sizeHint, "Almond")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['Almond', 'Butter']", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should scan all nodes of a label and not produce duplicates if nodes have multiple labels") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter", "Almond") ++
        nodeGraph(sizeHint, "Almond") ++
        nodeGraph(sizeHint, "Butter")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['Almond', 'Butter']", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("should handle non-existing labels") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['Honey', 'Almond', 'Butter']", Any)
      .build()

    // empty db
    val executablePlan = buildPlan(logicalQuery, runtime)
    execute(executablePlan) should beColumns("x").withNoRows()

    // CREATE Almond
    givenGraph(nodeGraph(sizeHint, "Almond"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(sizeHint))

    // CREATE Honey
    givenGraph(nodeGraph(sizeHint, "Honey"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(2 * sizeHint))

    // CREATE Butter
    givenGraph(nodeGraph(sizeHint, "Butter"))
    execute(executablePlan) should beColumns("x").withRows(rowCount(3 * sizeHint))
  }

  test("should scan all labels of all nodes") {
    // given
    val allThree = givenGraph {
      nodeGraph(sizeHint, "Butter")
      nodeGraph(sizeHint, "Almond")
      nodeGraph(sizeHint, "Honey")

      nodeGraph(sizeHint, "Butter", "Almond", "Honey")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['Honey', 'Almond', 'Butter']", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(allThree))
  }

  test("intersection scan on the RHS of union") {
    // given
    val cdNodes = givenGraph {
      nodeGraph(sizeHint, "C")
      nodeGraph(sizeHint, "D")

      nodeGraph(sizeHint, "C", "D")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .union()
      .|.filter("true")
      .|.dynamicLabelNodeLookup("x", "['C', 'D']", All, "x")
      .unwind("[1, 2] as x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(List(1, 2) ++ cdNodes))
  }

  test("intersection scan on the RHS of cartesian product") {
    // given
    val cdNodes = givenGraph {
      nodeGraph(sizeHint, "C")
      nodeGraph(sizeHint, "D")

      nodeGraph(sizeHint, "C", "D")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.filter("true")
      .|.dynamicLabelNodeLookup("y", "['C', 'D']", All)
      .unwind("[1, 2] as x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { ab <- List(1, 2); cd <- cdNodes } yield Array[Any](ab, cd)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should scan all nodes of a dynamic label from argument") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Butter") ++ nodeGraph(sizeHint, "Almond") ++ nodeGraph(sizeHint, "Honey")
    }

    val inputStream = inputValues(Array("Honey"), Array(Array("Almond", "Butter")))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.filter("true")
      .|.dynamicLabelNodeLookup("x", "lbl", Any, "lbl")
      .input(variables = Seq("lbl"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputStream)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("should handle empty array with Any") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "[]", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle empty array and filtered properties with Any") {
    givenGraph {
      nodeGraph(sizeHint)
      newNode("X", "age" -> 1)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "[]", Any, Map("age" -> "1"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle empty array with All") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "[]", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("should handle empty array and filtered properties with All") {
    val singleResult = givenGraph {
      nodeGraph(sizeHint)
      newNode("X", "age" -> 1)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "[]", All, Map("age" -> "1"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Array(singleResult)))
  }

  test("should handle nonexistent label with Any") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'NONEXISTENT_LABEL'", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle nonexistent label with All") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'NONEXISTENT_LABEL'", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should work with merge and a nonexistent label") {
    assume(!isParallel)
    assume(!isPipelined || canFuse)

    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults()
      .emptyResult()
      .merge(Seq(createNodeFull("n", dynamicLabels = Seq("'Foo'"))))
      .dynamicLabelNodeLookup("n", "'Foo'", All)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns().withStatistics(nodesCreated = 1, labelsAdded = 1)
  }

  test("should handle nonexistent label array with Any") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['NONEXISTENT_LABEL_1', 'NONEXISTENT_LABEL_2']", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle nonexistent label array with All") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['NONEXISTENT_LABEL_1', 'NONEXISTENT_LABEL_2']", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle single nonexistent label array with Any") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['NONEXISTENT_LABEL_1']", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle single nonexistent label array with All") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['NONEXISTENT_LABEL_1']", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should handle partially nonexistent label array with Any") {
    val foos = givenGraph {
      nodeGraph(sizeHint, "Foo")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['NONEXISTENT_LABEL_1', 'Foo']", Any)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(foos))
  }

  test("should handle partially nonexistent label array with All") {
    givenGraph {
      nodeGraph(sizeHint, "Foo")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['NONEXISTENT_LABEL_1', 'Foo']", All)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should check for null input in dynamic label expression") {
    givenGraph {
      tx.createNode()
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", NullCheckAssert(), Any)
      .build()

    the[Exception] thrownBy consume(execute(logicalQuery, runtime)) should not be a[NullCheckAssertException]
  }

  // LABEL + PROPERTY SEEKS

  test("should check for null input for the property expression map") {
    givenGraph {
      newNode("A")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", literalString("A"), Any, Map("prop" -> NullCheckAssert()))
      .build()

    the[Exception] thrownBy consume(execute(logicalQuery, runtime)) should not be a[NullCheckAssertException]
  }

  test("should filter for a single label and single property") {
    val expected = givenGraph {
      // not matched
      newNode("B", "prop" -> 1)
      newNode("A", "prop" -> 2)
      newNode("A", "paaaarp" -> 1)

      // matched
      Seq(
        newNode("A", "prop" -> 1),
        newNode("A", "B", "prop" -> 1),
        newNode("A", "B", "prop" -> 1, "paaaarp" -> 1)
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'A'", Any, Map("prop" -> "1"))
      .build()

    profile(logicalQuery, runtime) should beColumns("x").withRows(singleColumn(expected))
  }

  test("should filter for a single label and single property using an index") {
    val indexName = "the_index"
    val expected = givenGraph {
      nodeIndex("A")(_.on("prop").withName(indexName))

      // not matched
      newNode("B", "prop" -> 1)
      newNode("A", "prop" -> 2)
      newNode("A", "paaaarp" -> 1)

      // matched
      Seq(
        newNode("A", "prop" -> 1),
        newNode("A", "B", "prop" -> 1),
        newNode("A", "B", "prop" -> 1, "paaaarp" -> 1)
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'A'", Any, Map("prop" -> "1"))
      .build()

    profile(logicalQuery, runtime) should beColumns("x").withRows(singleColumn(expected)).usingIndexes(2, indexName)
  }

  test("should filter for a single label and multiple properties") {
    val expected = givenGraph {
      // not matched
      newNode("B", "prop" -> 1, "name" -> "bob")
      newNode("A", "prop" -> 2, "name" -> "bob")
      newNode("A", "prop" -> 1, "name" -> "alice")
      newNode("A", "prop" -> "bob", "name" -> 1)

      // matched
      Seq(
        newNode("A", "prop" -> 1, "name" -> "bob"),
        newNode("A", "B", "prop" -> 1, "name" -> "bob"),
        newNode("A", "prop" -> 1, "name" -> "bob", "notes" -> "jumentous")
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'A'", Any, Map("prop" -> "1", "name" -> "'bob'"))
      .build()

    profile(logicalQuery, runtime) should beColumns("x").withRows(singleColumn(expected))
  }

  test("should filter for a single label and multiple properties using a single index") {
    val indexName = "the_index"
    val expected = givenGraph {
      nodeIndex("A")(_.on("prop").withName(indexName))
      nodeIndex("B")(_.on("name").withName("unused_index"))

      // not matched
      newNode("B", "prop" -> 1, "name" -> "bob")
      newNode("A", "prop" -> 2, "name" -> "bob")
      newNode("A", "prop" -> 1, "name" -> "alice")
      newNode("A", "prop" -> "bob", "name" -> 1)

      // matched
      Seq(
        newNode("A", "prop" -> 1, "name" -> "bob"),
        newNode("A", "B", "prop" -> 1, "name" -> "bob"),
        newNode("A", "prop" -> 1, "name" -> "bob", "notes" -> "jumentous")
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'A'", Any, Map("prop" -> "1", "name" -> "'bob'"))
      .build()

    profile(logicalQuery, runtime) should beColumns("x").withRows(singleColumn(expected)).usingIndexes(2, indexName)
  }

  test("should filter for all labels and a single property") {
    val index_a = "A_prop"
    val index_b = "B_prop"

    val expected = givenGraph {
      nodeIndex("A")(_.on("prop").withName(index_a))
      nodeIndex("B")(_.on("prop").withName(index_b))
      nodeIndex("B", "paaaarp")

      // not matched
      newNode("A", "B", "prop" -> 2)
      newNode("A", "C", "prop" -> 1)
      newNode("A", "C", "D", "prop" -> 1)
      newNode("A", "B", "paaaarp" -> 1)

      // matched
      Seq(
        newNode("A", "B", "prop" -> 1),
        newNode("A", "B", "D", "prop" -> 1),
        newNode("A", "B", "prop" -> 1, "brrrap" -> 7)
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['A', 'B']", All, Map("prop" -> "1"))
      .build()

    profile(
      logicalQuery,
      runtime
    ) should beColumns("x").withRows(singleColumn(expected)).usingAnyIndexes(2, index_a, index_b)
  }

  test("should filter for all labels and multiple properties") {
    val expected = givenGraph {
      // not matched
      newNode("B", "age" -> 21, "name" -> "bob")
      newNode("A", "age" -> 22, "name" -> "bob")
      newNode("A", "B", "age" -> 22, "name" -> "bob")
      newNode("A", "age" -> 21, "name" -> "alice")
      newNode("A", "B", "age" -> 21, "name" -> "alice")
      newNode("A", "age" -> "bob", "name" -> 1)
      newNode("A", "age" -> 21, "name" -> "bob")
      newNode("A", "B", "age" -> 21)
      newNode("A", "B", "name" -> "bob")

      // matched
      Seq(
        newNode("A", "B", "age" -> 21, "name" -> "bob"),
        newNode("A", "B", "age" -> 21, "name" -> "bob", "notes" -> "jumentous"),
        newNode("A", "B", "C", "age" -> 21, "name" -> "bob")
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['B', 'A']", All, Map("age" -> "21", "name" -> "'bob'"))
      .build()

    profile(logicalQuery, runtime) should beColumns("x").withRows(singleColumn(expected))
  }

  test("should filter for multiple labels and multiple properties using any relevant index") {
    val compositeIndex = "the_composite_index"
    val ageIndex = "age_index"
    val nameIndex = "name_index"

    val expected = givenGraph {
      nodeIndex("A")(_.withName(ageIndex).on("age"))
      nodeIndex("A")(_.withName(nameIndex).on("name"))
      nodeIndex("B")(_.withName(compositeIndex).on("age").on("name"))

      // not matched
      newNode("B", "age" -> 21, "name" -> "bob")
      newNode("A", "age" -> 22, "name" -> "bob")
      newNode("A", "B", "age" -> 22, "name" -> "bob")
      newNode("A", "age" -> 21, "name" -> "alice")
      newNode("A", "B", "age" -> 21, "name" -> "alice")
      newNode("A", "age" -> "bob", "name" -> 1)
      newNode("A", "age" -> 21, "name" -> "bob")
      newNode("A", "B", "age" -> 21)
      newNode("A", "B", "name" -> "bob")

      // matched
      Seq(
        newNode("A", "B", "age" -> 21, "name" -> "bob"),
        newNode("A", "B", "age" -> 21, "name" -> "bob", "notes" -> "jumentous"),
        newNode("A", "B", "C", "age" -> 21, "name" -> "bob")
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "['A', 'B']", All, Map("age" -> "21", "name" -> "'bob'"))
      .build()

    val (res, prof) = executeAndProfile(logicalQuery, runtime)

    res should beColumns("x").withRows(singleColumn(expected))
      .usingAnyIndexes(2, compositeIndex, ageIndex, nameIndex)

    val profiledIndexes = prof.find("DynamicLabelNodeLookup")
      .head
      .arguments
      .collectFirst { case UsedIndexes(indexes) => indexes }

    profiledIndexes shouldBe defined
    profiledIndexes.get.values.sum shouldBe 1
  }

  test("should pass property predicates to the kernel seek in the same order as they are defined in the index") {
    val compositeIndex = "the_composite_index"
    val ageIndex = "age_index"
    val nameIndex = "name_index"

    val expected = givenGraph {
      nodeIndex("A")(_.withName(compositeIndex).on("age").on("name"))

      // not matched
      newNode("B", "age" -> 21, "name" -> "bob")
      newNode("A", "age" -> 22, "name" -> "bob")
      newNode("A", "B", "age" -> 21)
      newNode("A", "B", "name" -> "bob")

      // matched
      Seq(
        newNode("A", "age" -> 21, "name" -> "bob"),
        newNode("A", "B", "age" -> 21, "name" -> "bob", "notes" -> "jumentous"),
        newNode("A", "B", "name" -> "bob", "age" -> 21)
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'A'", All, ListMap("name" -> "'bob'", "age" -> "21"))
      .build()

    val (res, prof) = executeAndProfile(logicalQuery, runtime)

    res should beColumns("x").withRows(singleColumn(expected))
      .usingAnyIndexes(2, compositeIndex, ageIndex, nameIndex)

    val profiledIndexes = prof.find("DynamicLabelNodeLookup")
      .head
      .arguments
      .collectFirst { case UsedIndexes(indexes) => indexes }

    profiledIndexes shouldBe defined
    profiledIndexes.get.values.sum shouldBe 1
  }

  test("should not try to use indexes that can't support the intended query") {
    val indexName = "the_index"
    val expected = givenGraph {
      nodeIndex("A")(_.on("prop").withName(indexName).withIndexType(IndexType.POINT))

      // not matched
      newNode("B", "prop" -> 1)
      newNode("A", "prop" -> 2)
      newNode("A", "paaaarp" -> 1)

      // matched
      Seq(
        newNode("A", "prop" -> 1),
        newNode("A", "B", "prop" -> 1),
        newNode("A", "B", "prop" -> 1, "paaaarp" -> 1)
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .dynamicLabelNodeLookup("x", "'A'", Any, Map("prop" -> "1"))
      .build()

    val (res, prof) = executeAndProfile(logicalQuery, runtime)

    res should beColumns("x").withRows(singleColumn(expected))
      .notUsingIndexes(2, indexName)

    val args = prof.find("DynamicLabelNodeLookup")
      .head
      .arguments

    all(args) should not be a[UsedIndexes]
  }

}
