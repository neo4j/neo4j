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
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueType
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.byteValue
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.float32Vector
import org.neo4j.values.storable.Values.float64Vector
import org.neo4j.values.storable.Values.floatValue
import org.neo4j.values.storable.Values.int16Vector
import org.neo4j.values.storable.Values.int32Vector
import org.neo4j.values.storable.Values.int64Vector
import org.neo4j.values.storable.Values.int8Vector
import org.neo4j.values.storable.Values.intValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.shortValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable.VectorValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import java.time.Duration
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneOffset

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.comparatorToOrdering

//noinspection ScalaDeprecation,RedundantDefaultArgument
abstract class RelationshipVectorIndexSearchTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition,
      runtime
    ) with QueryExpressionConstructionTestSupport {

  private val seed: Long = System.currentTimeMillis()
  println(s"initial seed: $seed")
  private val random = RandomValues.create(new java.util.Random(seed))
  private def randomVector = random.nextFloat32Vector(1536, 1536)

  private def relationshipGraph(size: Int, typ: String): Seq[Relationship] = {
    val relationships = ArrayBuffer.empty[Relationship]
    val tx = runtimeTestSupport.tx
    (1 to size).foreach { _ =>
      {
        relationships += tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName(typ))
      }
    }
    relationships.toSeq
  }

  private val configurations: Seq[(String, VectorValue)] = Seq(
    ("INT64", int64Vector(1L to sizeHint: _*)),
    ("INT32", int32Vector(1 to sizeHint: _*)),
    ("INT16", int16Vector((1 to sizeHint).map(_.toShort): _*)),
    ("INT8", int8Vector((1 to sizeHint).map(_.toByte): _*)),
    ("FLOAT64", float64Vector((1 to sizeHint).map(_.toDouble): _*)),
    ("FLOAT32", float32Vector((1 to sizeHint).map(_.toFloat): _*))
  )

  test("should project nodes from a directed search") {
    // given
    val (a, r, b) = givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val a = tx.createNode()
      val b = tx.createNode()
      val r = a.createRelationshipTo(b, RelationshipType.withName("Foo"))
      r.setProperty("v", float32Vector(1, 2, 3))
      (a, r, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipVectorIndexSearch(
        "(a)-[r]->(b)",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "[1, 2, 3]",
        limit = s"10000000"
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "r", "b").withSingleRow(a, r, b)
  }

  test("should project nodes from a undirected search") {
    // given
    val (a, r, b) = givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val a = tx.createNode()
      val b = tx.createNode()
      val r = a.createRelationshipTo(b, RelationshipType.withName("Foo"))
      r.setProperty("v", float32Vector(1, 2, 3))
      (a, r, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipVectorIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "[1, 2, 3]",
        limit = s"10000000"
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "r", "b").withRows(Seq(Array(a, r, b), Array(b, r, a)))
  }

  test("should only find one row if self-loop") {
    // given
    val (a, r) = givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val a = tx.createNode()
      val r = a.createRelationshipTo(a, RelationshipType.withName("Foo"))
      r.setProperty("v", float32Vector(1, 2, 3))
      (a, r)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipVectorIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "[1, 2, 3]",
        limit = s"10000000"
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "r", "b").withSingleRow(a, r, a)
  }

  configurations.foreach {
    case (name, vector) =>
      test(s"$name directed vector index search with score variable") {
        givenGraph {
          relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          relationshipGraph(1, "Foo").foreach(r => {
            r.setProperty("id", 1)
            r.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id", "score")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            "()-[r]->()",
            typeNames = Seq("Foo"),
            properties = Seq("v"),
            indexName = "VectorIndex",
            vector = "$vector",
            limit = "20",
            score = "score"
          )
          .build()

        // then
        val runtimeResult = execute(logicalQuery, runtime, parameters = Map("vector" -> vector))
        runtimeResult should beColumns("id", "score").withRows(matching {
          case Seq(Array(id, score: NumberValue)) if id == longValue(1) && tolerantEquals(1.0, score.doubleValue()) =>
        })
      }

      test(s"$name undirected vector index search with score variable") {
        givenGraph {
          relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          relationshipGraph(1, "Foo").foreach(r => {
            r.setProperty("id", 1)
            r.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id", "score")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            "()-[r]-()",
            typeNames = Seq("Foo"),
            properties = Seq("v"),
            indexName = "VectorIndex",
            vector = "$vector",
            limit = "20",
            score = "score"
          )
          .build()

        // then
        val runtimeResult = execute(logicalQuery, runtime, parameters = Map("vector" -> vector))
        runtimeResult should beColumns("id", "score").withRows(matching {
          case Seq(
              Array(id1, score1: NumberValue),
              Array(id2, score2: NumberValue)
            )
            if id1 == longValue(1) && id2 == longValue(1) && tolerantEquals(
              1.0,
              score1.doubleValue()
            ) && tolerantEquals(1.0, score2.doubleValue()) =>
        })
      }

      test(s"$name directed vector index search without score variable") {
        givenGraph {
          relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          relationshipGraph(1, "Foo").foreach(r => {
            r.setProperty("id", 1)
            r.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            "()-[r]->()",
            typeNames = Seq("Foo"),
            properties = Seq("v"),
            indexName = "VectorIndex",
            vector = "$vector",
            limit = "20"
          )
          .build()

        // then
        val runtimeResult = execute(logicalQuery, runtime, parameters = Map("vector" -> vector))
        runtimeResult should beColumns("id").withSingleRow(1)
      }

      test(s"$name undirected vector index search without score variable") {
        givenGraph {
          relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          relationshipGraph(1, "Foo").foreach(r => {
            r.setProperty("id", 1)
            r.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            "()-[r]-()",
            typeNames = Seq("Foo"),
            properties = Seq("v"),
            indexName = "VectorIndex",
            vector = "$vector",
            limit = "20"
          )
          .build()

        // then
        val runtimeResult = execute(logicalQuery, runtime, parameters = Map("vector" -> vector))
        runtimeResult should beColumns("id").withRows(singleColumn(Seq(1, 1)))
      }

      test(
        s"$name should be able to do directed search using a list of integers instead of an explicit vector"
      ) {
        givenGraph {
          relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          relationshipGraph(1, "Foo").foreach(r => {
            r.setProperty("id", 1)
            r.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id", "score")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            "()-[r]->()",
            typeNames = Seq("Foo"),
            properties = Seq("v"),
            indexName = "VectorIndex",
            vector = s"${vectorAsCypherList(vector)}",
            limit = "20",
            score = "score"
          ).build()

        val runtimeResult = execute(logicalQuery, runtime, parameters = Map("vector" -> vector))
        runtimeResult should beColumns("id", "score").withRows(matching {
          case Seq(Array(id, score: NumberValue)) if id == longValue(1) && tolerantEquals(1.0, score.doubleValue()) =>
        })
      }

      test(
        s"$name should be able to do undirected search using a list of integers instead of an explicit vector"
      ) {
        givenGraph {
          relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          relationshipGraph(1, "Foo").foreach(r => {
            r.setProperty("id", 1)
            r.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id", "score")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            "()-[r]-()",
            typeNames = Seq("Foo"),
            properties = Seq("v"),
            indexName = "VectorIndex",
            vector = s"${vectorAsCypherList(vector)}",
            limit = "20",
            score = "score"
          ).build()

        val runtimeResult = execute(logicalQuery, runtime, parameters = Map("vector" -> vector))
        runtimeResult should beColumns("id", "score").withRows(matching {
          case Seq(
              Array(id1, score1: NumberValue),
              Array(id2, score2: NumberValue)
            )
            if id1 == longValue(1) && id2 == longValue(1) && tolerantEquals(
              1.0,
              score1.doubleValue()
            ) && tolerantEquals(1.0, score2.doubleValue()) =>
        })
      }
  }

  test("should be able to do a directed query of the index with multiple inputs from a property") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val limit = 11
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r1.id AS id")
      .apply()
      .|.relationshipVectorIndexSearch(
        "()-[r2]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "r1.v",
        limit = s"$limit",
        score = "score",
        argumentIds = Set("r1")
      )
      .filter("r1.id < 20")
      .relationshipTypeScan("()-[r1:Foo]->()")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id", "score").withRows(rowCount(20 * limit))
  }

  test(
    "should be able to do a undirected query of the index with multiple inputs from a property"
  ) {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val limit = 11
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r1.id AS id")
      .apply()
      .|.relationshipVectorIndexSearch(
        "()-[r2]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "r1.v",
        limit = s"$limit",
        score = "score",
        argumentIds = Set("r1")
      )
      .filter("r1.id < 20")
      .relationshipTypeScan("()-[r1:Foo]->()")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id", "score").withRows(rowCount(2 * 20 * limit))
  }

  test("should fail if index doesn't exists") {
    val theVector = float32Vector((1 to sizeHint).map(_.toFloat): _*)
    givenGraph {
      relationshipGraph(1, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", theVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(theVector)}",
        limit = "13",
        score = "score"
      ).build()

    // then
    the[IndexNotFoundKernelException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22N69,
      "error: data exception - index does not exist. The index 'VectorIndex' does not exist."
    )
  }

  test("should fail if index isn't a vector index") {
    val theVector = float32Vector((1 to sizeHint).map(_.toFloat): _*)
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.RANGE, Seq("Foo"), "v")
      relationshipGraph(1, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", "theVector")
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(theVector)}",
        limit = "13",
        score = "score"
      ).build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime
    )) should have message "22NCG: Expected the index `VectorIndex` to be a vector index but was a range index."
  }

  test("should fail if search item has the wrong type") {
    val theVector = float32Vector((1 to sizeHint).map(_.toFloat): _*)
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(1, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", theVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"'THIS IS NOT A VECTOR'",
        limit = "13",
        score = "score"
      ).build()

    // then
    the[CypherTypeException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22G03,
      "error: data exception - invalid value type"
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N01,
      "error: data exception - invalid type.",
      fuzzyStatusDescr = true
    )
  }

  test("should return empty if search item is null") {
    val theVector = float32Vector((1 to sizeHint).map(_.toFloat): _*)
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(1, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", theVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "NULL",
        limit = "13",
        score = "score"
      ).build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id", "score").withNoRows()
  }

  test("should fail if search item is a list containing null") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(1, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", int32Vector(1, 2, 3))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"[1, NULL, 3]",
        limit = "13",
        score = "score"
      ).build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22NBG,
      "error: data exception - invalid vector coordinates. Invalid vector coordinates. The vector coordinates must be finite."
    )
  }

  test("should respect the limit (directed)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13",
        score = "score"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> randomVector))
    runtimeResult should beColumns("id", "score").withRows(rowCount(13))
  }

  test("should respect the limit (undirected)") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13",
        score = "score"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> randomVector))
    runtimeResult should beColumns("id", "score").withRows(rowCount(26))
  }

  test("should handle limit 0 (directed)") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "0",
        score = "score"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> randomVector))
    runtimeResult should beColumns("id", "score").withNoRows()
  }

  test("should handle limit 0 (undirected)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "0",
        score = "score"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> randomVector))
    runtimeResult should beColumns("id", "score").withNoRows()
  }

  test("should fail on negative limits (directed)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "-1",
        score = "score"
      ).build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime,
      parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*))
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value -1 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'value' to be of type INTEGER and in the range 0 to 9223372036854775807 but found -1."
    )
  }

  test("should fail on negative limits (undirected)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "-1",
        score = "score"
      ).build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime,
      parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*))
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value -1 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'value' to be of type INTEGER and in the range 0 to 9223372036854775807 but found -1."
    )
  }

  test("should fail on too large limits (directed)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "9223372036854775807",
        score = "score"
      ).build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime,
      parameters = Map("vector" -> randomVector)
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value 9223372036854775807 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'LIMIT' to be of type INTEGER NOT NULL and in the range 0 to 2147483647 but found 9223372036854775807."
    )
  }

  test("should fail on too large limits (undirected)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("v", randomVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "9223372036854775807",
        score = "score"
      ).build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime,
      parameters = Map("vector" -> randomVector)
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value 9223372036854775807 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'LIMIT' to be of type INTEGER NOT NULL and in the range 0 to 2147483647 but found 9223372036854775807."
    )
  }

  test("should support multiple types (directed)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo", "Bar", "Baz"), "v")
      relationshipGraph(1, "Foo").foreach(r =>
        r.setProperty("v", randomVector)
      )
      relationshipGraph(1, "Bar").foreach(r =>
        r.setProperty("v", randomVector)
      )
      relationshipGraph(1, "Baz").foreach(r =>
        r.setProperty("v", randomVector)
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("type")
      .projection("type(r) AS type")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> randomVector))
    runtimeResult should beColumns("type").withRows(singleColumn(Seq("Foo", "Bar", "Baz")))
  }

  test("should support multiple types (undirected)") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo", "Bar", "Baz"), "v")
      relationshipGraph(1, "Foo").foreach(r =>
        r.setProperty("v", randomVector)
      )
      relationshipGraph(1, "Bar").foreach(r =>
        r.setProperty("v", randomVector)
      )
      relationshipGraph(1, "Baz").foreach(r =>
        r.setProperty("v", randomVector)
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("type")
      .projection("type(r) AS type")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> randomVector))
    runtimeResult should beColumns("type").withRows(singleColumn(Seq("Foo", "Foo", "Bar", "Bar", "Baz", "Baz")))
  }

  // TODO: arrays and temporals not supported yet
  val ranges: Seq[Int => Value] = Seq(
    longValue(_),
    intValue,
    i => shortValue(i.shortValue()),
    i => byteValue((Byte.MinValue + i).byteValue),
    i => doubleValue(i.doubleValue()),
    i => floatValue(i.floatValue()),
    i => stringValue("A".repeat(i)),
    i => LocalTimeValue.localTime(LocalTime.ofSecondOfDay(i)),
    i => TimeValue.time(OffsetTime.of(LocalTime.ofSecondOfDay(i), ZoneOffset.UTC)),
    i => LocalDateTimeValue.localDateTime(i, 1, 1, 0, 0, 0, 0),
    i => DateTimeValue.datetime(i, 1, 1, 0, 0, 0, 0, "UTC"),
    i => DateValue.date(i, 1, 1)
  )

  ranges.foreach(range => {
    // we use a size small enough to have unique sequence of byte values
    val size = 256
    def createGraph(): Unit = {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(size, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, range(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    val indexToSearch = random.nextInt(size)
    val valueToSearch = range(indexToSearch)

    test(
      s"should support single-stage filtering single exact directed search n.id = $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "13",
          propertyFilter = Some(single(param("seekValue")))
        ).build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withSingleRow(valueToSearch)
    }

    test(
      s"should support single-stage filtering single exact undirected search n.id = $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "13",
          propertyFilter = Some(single(param("seekValue")))
        ).build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withRows(singleColumn(Seq(valueToSearch, valueToSearch)))
    }

    test(
      s"should support single-stage filtering single directed range search $valueToSearch <= n.id <= $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("seekValue")), lte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withSingleRow(valueToSearch)
    }

    test(
      s"should support single-stage filtering single undirected range search $valueToSearch <= n.id <= $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("seekValue")), lte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withRows(singleColumn(Seq(valueToSearch, valueToSearch)))
    }

    test(
      s"should support single-stage filtering single range direct search $valueToSearch < n.id <= $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gt(param("seekValue")), lte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single range undirect search $valueToSearch < n.id <= $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gt(param("seekValue")), lte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single direct range search $valueToSearch <= n.id < $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("seekValue")), lt(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single undirect range search $valueToSearch <= n.id < $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("seekValue")), lt(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single directed range seek $valueToSearch < n.id < $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gt(param("seekValue")), lt(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single undirected range seek $valueToSearch < n.id < $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gt(param("seekValue")), lt(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single directed range search ${range(size - 1)} <= n.id <= ${range(0)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("min")), lte(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(size - 1),
              "max" -> range(0)
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single undirected range search ${range(size - 1)} <= n.id <= ${range(0)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("min")), lte(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(size - 1),
              "max" -> range(0)
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single directed range search ${range(0)} <= n.id < ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("min")), lt(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (0 until size - 1).map(range)
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single undirected range search ${range(0)} <= n.id < ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("min")), lt(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (0 until size - 1).map(range).flatMap(i => Seq(i, i))
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single directed range search ${range(0)} < n.id <= ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gt(param("min")), lte(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (1 until size).map(range)
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single undirected range search ${range(0)} < n.id <= ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gt(param("min")), lte(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (1 until size).map(range).flatMap(i => Seq(i, i))
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single directed range search ${range(0)} <= n.id <= ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("min")), lte(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (0 until size).map(range)
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single undirected range search ${range(0)} <= n.id <= ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("r.id AS id")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(between(gte(param("min")), lte(param("max"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (0 until size).map(range).flatMap(i => Seq(i, i))
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single directed range search $valueToSearch < n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id > $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(gt(param("seekValue"))))
        )
        .build()

      val runtimeResult = {
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )
      }

      // then
      val expectedNumberOfRows = size - indexToSearch - 1
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single undirected range search $valueToSearch < n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id > $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(gt(param("seekValue"))))
        )
        .build()

      val runtimeResult = {
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )
      }

      // then
      val expectedNumberOfRows = 2 * (size - indexToSearch - 1)
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single directed range search $valueToSearch <= n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id >= $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(gte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = size - indexToSearch
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single undirected range search $valueToSearch <= n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id >= $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(gte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = 2 * (size - indexToSearch)
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single directed range search $valueToSearch > n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id < $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(lt(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = indexToSearch
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single undirected range search $valueToSearch > n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id < $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(lt(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = 2 * indexToSearch
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single directed range search $valueToSearch >= n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id <= $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(lte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = indexToSearch + 1
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(
      s"should support single-stage filtering single undirected range search $valueToSearch >= n.id"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("result")
        .projection("r.id <= $seekValue AS result")
        .relationshipVectorIndexSearch(
          "()-[r]-()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = "$vector",
          limit = "1000000",
          propertyFilter = Some(rangeExpression(lte(param("seekValue"))))
        )
        .build()

      val runtimeResult =
        execute(
          logicalQuery,
          runtime,
          parameters =
            Map(
              "vector" -> randomVector,
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = 2 * (indexToSearch + 1)
      runtimeResult should beColumns("result").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }
  })

  test("should support single-stage filtering single directed range search with different types") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(between(gte(param("seekValue1")), lte(param("seekValue2"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue1" -> longValue(0),
            "seekValue2" -> stringValue("10000")
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test(
    "should support single-stage filtering single undirected range search with different types"
  ) {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(between(gte(param("seekValue1")), lte(param("seekValue2"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue1" -> longValue(0),
            "seekValue2" -> stringValue("10000")
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test(
    "should support single-stage filtering single directed range search with different non-storable types"
  ) {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(between(gte(param("seekValue1")), lte(param("seekValue2"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue1" -> longValue(0),
            "seekValue2" -> EMPTY_MAP
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test(
    "should support single-stage filtering single undirected range search with different non-storable types"
  ) {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(between(gte(param("seekValue1")), lte(param("seekValue2"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue1" -> longValue(0),
            "seekValue2" -> EMPTY_MAP
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test(
    "should support single-stage filtering single directed open range search with non-storable type"
  ) {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(rangeExpression(gte(param("seekValue"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue" -> EMPTY_MAP
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test(
    "should support single-stage filtering single undirected open range search with non-storable type"
  ) {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(rangeExpression(gte(param("seekValue"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue" -> EMPTY_MAP
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test("should support single-stage filtering single directed range search between null values") {

    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(between(gte(param("seekValue1")), lte(param("seekValue2"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue1" -> null,
            "seekValue2" -> null
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test("should support single-stage filtering single undirected range search between null values") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "1000000",
        propertyFilter = Some(between(gte(param("seekValue1")), lte(param("seekValue2"))))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "seekValue1" -> null,
            "seekValue2" -> null
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test("should support single-stage filtering single range seek of booleans") {
    // given
    givenGraph(booleanVectorGraph(20))

    // [FALSE, TRUE] < n.bool < [FALSE, TRUE]
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = false,
      max = true
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = true,
      max = true
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = true,
      max = false
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = false,
      max = false
    ) should beColumns("result").withNoRows()
    // [FALSE, TRUE] <= n.bool < [FALSE, TRUE]
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = false,
      max = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false)))
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = true,
      max = true
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = true,
      max = false
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = false,
      max = false
    ) should beColumns("result").withNoRows()
    // [FALSE, TRUE] < n.bool <= [FALSE, TRUE]
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = false,
      max = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(true)))
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = true,
      max = true
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = true,
      max = false
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = false,
      max = false
    ) should beColumns("result").withNoRows()
    // [FALSE, TRUE] <= n.bool <= [FALSE, TRUE]
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = false,
      max = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false) ++ Seq.fill(10)(true)))
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = true,
      max = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(true)))
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = true,
      max = false
    ) should beColumns("result").withNoRows()
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = false,
      max = false
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false)))

    // [FALSE, TRUE] <= n.bool
    executeBooleanPlan(
      rangeExpression(gte(param("min"))),
      min = false
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false) ++ Seq.fill(10)(true)))
    executeBooleanPlan(
      rangeExpression(gte(param("min"))),
      min = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(true)))
    // [FALSE, TRUE] < n.bool
    executeBooleanPlan(
      rangeExpression(gt(param("min"))),
      min = false
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(true)))
    executeBooleanPlan(rangeExpression(gt(param("min"))), min = true) should beColumns("result").withNoRows()
    // [FALSE, TRUE] >= n.bool
    executeBooleanPlan(
      rangeExpression(lte(param("min"))),
      min = false
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false)))
    executeBooleanPlan(
      rangeExpression(lte(param("min"))),
      min = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false) ++ Seq.fill(10)(true)))
    // [FALSE, TRUE] > n.bool
    executeBooleanPlan(rangeExpression(lt(param("min"))), min = false) should beColumns("result").withNoRows()
    executeBooleanPlan(
      rangeExpression(lt(param("min"))),
      min = true
    ) should beColumns("result").withRows(singleColumn(Seq.fill(10)(false)))
  }

  /**
   * Tests so that we get equivalent results from a normal range index query
   * compared to what we get from a filtering vector stage query.
   */

  (1 to 100).foreach(i => {
    val dimension = random.intBetween(128, 1028)
    def randomVector = random.nextFloat32Vector(dimension, dimension)
    def randomValue: Value = random.nextValueOfTypes(
      ValueType.BOOLEAN,
      ValueType.STRING,
      ValueType.BYTE,
      ValueType.SHORT,
      ValueType.INT,
      ValueType.LONG,
      ValueType.FLOAT,
      ValueType.DOUBLE,
      ValueType.LOCAL_DATE_TIME,
      ValueType.DATE_TIME,
      ValueType.LOCAL_TIME,
      ValueType.TIME,
      ValueType.DATE,
      ValueType.DURATION
    )
    val Seq(min, max) = Seq(randomValue, randomValue).sorted(comparatorToOrdering(Values.COMPARATOR))
    def randomSearchPredicate = {
      random.nextInt(6) match {
        case 0 => (rangeExpression(gt(param("min"))), s"n.prop > $min")
        case 1 => (rangeExpression(gte(param("min"))), s"n.prop >= $min")
        case 2 => (rangeExpression(lt(param("min"))), s"n.prop < $min")
        case 3 => (rangeExpression(lte(param("min"))), s"n.prop <= $min")
        case 4 => (between(gte(param("min")), lte(param("max"))), s"$min <= n.prop <= $max")
        case 5 => (single(param("min")), s"n.prop = $min")
        case _ => throw new IllegalStateException
      }
    }
    val (predicate, predicateString) = randomSearchPredicate
    val searchVector = randomVector
    val directed = random.nextBoolean()

    def run(query: LogicalQuery) = {
      consume(execute(
        query,
        runtime,
        parameters =
          Map(
            "vector" -> searchVector,
            "min" -> min,
            "max" -> max
          )
      )).map(_(0))
    }

    def randomGraph(): Unit = {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      relationshipIndex("RangeIndex", IndexType.RANGE, Seq("Foo"), "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").foreach({
        r =>
          write.relationshipSetProperty(r.getId, idToken, randomValue)
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    test(
      s"index equivalence test iteration=$i, directed=$directed"
    ) {
      withClue(s"seed=$seed predicate=$predicateString") {
        // given
        givenGraph(randomGraph())

        // when
        def rightArrow = if (directed) "->" else "-"

        val vectorQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("r.id AS id")
          .relationshipVectorIndexSearch(
            s"()-[r]$rightArrow()",
            typeNames = Seq("Foo"),
            properties = Seq("v", "id"),
            indexName = "VectorIndex",
            vector = "$vector",
            limit = s"10000000",
            propertyFilter = Some(predicate)
          )
          .build()

        val rangeQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("r.id AS id")
          .relationshipIndexOperator(s"()-[r:Foo(id)]$rightArrow()", customQueryExpression = Some(predicate))
          .build()

        // then
        run(vectorQuery) should contain theSameElementsAs run(rangeQuery)
      }
    }
  })

  test("comparing floating point and integer") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(1, "Foo").foreach({
        r =>
          write.relationshipSetProperty(r.getId, idToken, longValue(1))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(single(param("p")))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p" -> 1.1
          )
      )

    // then
    runtimeResult should beColumns("id").withNoRows()
  }

  test("exact filter for non-storable value") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(1, "Foo").foreach({
        r =>
          write.relationshipSetProperty(r.getId, idToken, longValue(1))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(single(param("p")))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p" -> EMPTY_MAP
          )
      )

    // then
    runtimeResult should beColumns("id").withNoRows()
  }

  test("should support multiple composite directed exact filters") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    val toFind = random.nextInt(sizeHint)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(single(param("p1")), single(param("p2"))))
      )
      .build()

    def run(p1: AnyValue, p2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p1" -> p1,
            "p2" -> p2
          )
      )
    // then
    run(
      longValue(toFind),
      stringValue(toFind.toString)
    ) should beColumns("id1", "id2").withSingleRow(toFind, toFind.toString)
    run(longValue(0), stringValue("1")) should beColumns("id1", "id2").withNoRows()
    run(NO_VALUE, stringValue(toFind.toString)) should beColumns("id1", "id2").withNoRows()
    run(longValue(toFind), NO_VALUE) should beColumns("id1", "id2").withNoRows()
    run(NO_VALUE, NO_VALUE) should beColumns("id1", "id2").withNoRows()
    run(EMPTY_MAP, stringValue(toFind.toString)) should beColumns("id1", "id2").withNoRows()
    run(longValue(toFind), EMPTY_MAP) should beColumns("id1", "id2").withNoRows()
    run(EMPTY_MAP, EMPTY_MAP) should beColumns("id1", "id2").withNoRows()
  }

  test("should support multiple composite undirected exact filters") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    val toFind = random.nextInt(sizeHint)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(single(param("p1")), single(param("p2"))))
      )
      .build()

    def run(p1: AnyValue, p2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p1" -> p1,
            "p2" -> p2
          )
      )
    // then
    run(
      longValue(toFind),
      stringValue(toFind.toString)
    ) should beColumns("id1", "id2").withRows(Seq(Array(toFind, toFind.toString), Array(toFind, toFind.toString)))
    run(longValue(0), stringValue("1")) should beColumns("id1", "id2").withNoRows()
    run(NO_VALUE, stringValue(toFind.toString)) should beColumns("id1", "id2").withNoRows()
    run(longValue(toFind), NO_VALUE) should beColumns("id1", "id2").withNoRows()
    run(NO_VALUE, NO_VALUE) should beColumns("id1", "id2").withNoRows()
    run(EMPTY_MAP, stringValue(toFind.toString)) should beColumns("id1", "id2").withNoRows()
    run(longValue(toFind), EMPTY_MAP) should beColumns("id1", "id2").withNoRows()
    run(EMPTY_MAP, EMPTY_MAP) should beColumns("id1", "id2").withNoRows()
  }

  test("should support exact composite directed query with one gap") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(single(param("p1")), single(param("p2")), AllQueryExpression))
      )
      .build()

    def run(p1: AnyValue, p2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p1" -> p1,
            "p2" -> p2
          )
      )

    // then
    run(
      longValue(128),
      stringValue("128")
    ) should beColumns("id1", "id2", "id3").withSingleRow(128, "128", -128)
    run(
      longValue(2),
      stringValue("42")
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      NO_VALUE,
      stringValue("42")
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      longValue(42),
      NO_VALUE
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      NO_VALUE,
      NO_VALUE
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      EMPTY_MAP,
      stringValue("42")
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      longValue(42),
      EMPTY_MAP
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      EMPTY_MAP,
      EMPTY_MAP
    ) should beColumns("id1", "id2", "id3").withNoRows()
  }

  test("should support exact composite undirected query with one gap") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(single(param("p1")), single(param("p2")), AllQueryExpression))
      )
      .build()

    def run(p1: AnyValue, p2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p1" -> p1,
            "p2" -> p2
          )
      )

    // then
    run(
      longValue(128),
      stringValue("128")
    ) should beColumns("id1", "id2", "id3").withRows(Seq.fill(2)(Array(128, "128", -128)))
    run(
      longValue(2),
      stringValue("42")
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      NO_VALUE,
      stringValue("42")
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      longValue(42),
      NO_VALUE
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      NO_VALUE,
      NO_VALUE
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      EMPTY_MAP,
      stringValue("42")
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      longValue(42),
      EMPTY_MAP
    ) should beColumns("id1", "id2", "id3").withNoRows()
    run(
      EMPTY_MAP,
      EMPTY_MAP
    ) should beColumns("id1", "id2", "id3").withNoRows()
  }

  test("should support exact composite directed query with two gaps") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(AllQueryExpression, AllQueryExpression, single(param("p"))))
      )
      .build()

    def run(p: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p" -> p
          )
      )

    // then
    run(byteValue(42)) should beColumns("id1", "id2", "id3").withRows(
      Seq(
        Array(42, "42", 42),
        Array(298, "298", 42),
        Array(554, "554", 42),
        Array(810, "810", 42)
      )
    )

    run(NO_VALUE) should beColumns("id1", "id2", "id3").withNoRows()
    run(EMPTY_MAP) should beColumns("id1", "id2", "id3").withNoRows()
  }

  test("should support exact composite undirected query with two gaps") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(AllQueryExpression, AllQueryExpression, single(param("p"))))
      )
      .build()

    def run(p: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "p" -> p
          )
      )

    // then
    run(byteValue(42)) should beColumns("id1", "id2", "id3").withRows(
      Seq(
        Array(42, "42", 42),
        Array(42, "42", 42),
        Array(298, "298", 42),
        Array(298, "298", 42),
        Array(554, "554", 42),
        Array(554, "554", 42),
        Array(810, "810", 42),
        Array(810, "810", 42)
      )
    )

    run(NO_VALUE) should beColumns("id1", "id2", "id3").withNoRows()
    run(EMPTY_MAP) should beColumns("id1", "id2", "id3").withNoRows()
  }

  test("should support exact composite directed query with just gaps") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(AllQueryExpression, AllQueryExpression, AllQueryExpression))
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("id1", "id2", "id3").withRows(rowCount(1000))
  }

  test("should support exact composite undirected query with just gaps") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(AllQueryExpression, AllQueryExpression, AllQueryExpression))
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("id1", "id2", "id3").withRows(rowCount(2 * 1000))
  }

  test("should support multiple composite directed range filters") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          between(gte(param("from1")), lte(param("to1"))),
          between(gte(param("from2")), lte(param("to2")))
        ))
      )
      .build()

    def run(from1: AnyValue, to1: AnyValue, from2: AnyValue, to2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "from1" -> from1,
            "to1" -> to1,
            "from2" -> from2,
            "to2" -> to2
          )
      )

    // then
    run(
      from1 = longValue(10),
      to1 = longValue(20),
      from2 = stringValue("12"),
      to2 = stringValue("22")
    ) should beColumns("id1", "id2").withRows(
      (12 to 20).map(i => Array(i, i.toString))
    )
    run(
      from1 = longValue(10),
      to1 = longValue(20),
      from2 = stringValue("22"),
      to2 = stringValue("24")
    ) should beColumns("id1", "id2").withNoRows()
    run(
      from1 = longValue(20),
      to1 = longValue(10),
      from2 = stringValue("10"),
      to2 = stringValue("20")
    ) should beColumns("id1", "id2").withNoRows()
    Seq(NO_VALUE, EMPTY_MAP).map(impossibleValue => {
      run(
        from1 = impossibleValue,
        to1 = longValue(20),
        from2 = stringValue("10"),
        to2 = stringValue("20")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from1 = longValue(10),
        to1 = impossibleValue,
        from2 = stringValue("10"),
        to2 = stringValue("20")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from1 = longValue(10),
        to1 = longValue(20),
        from2 = impossibleValue,
        to2 = stringValue("20")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from1 = longValue(10),
        to1 = longValue(20),
        from2 = stringValue("10"),
        to2 = impossibleValue
      ) should beColumns("id1", "id2").withNoRows()
    })
  }

  test("should support multiple composite undirected range filters") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          between(gte(param("from1")), lte(param("to1"))),
          between(gte(param("from2")), lte(param("to2")))
        ))
      )
      .build()

    def run(from1: AnyValue, to1: AnyValue, from2: AnyValue, to2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "from1" -> from1,
            "to1" -> to1,
            "from2" -> from2,
            "to2" -> to2
          )
      )

    // then
    run(
      from1 = longValue(10),
      to1 = longValue(20),
      from2 = stringValue("12"),
      to2 = stringValue("22")
    ) should beColumns("id1", "id2").withRows(
      (12 to 20).map(i => Array(i, i.toString))
    )
    run(
      from1 = longValue(10),
      to1 = longValue(20),
      from2 = stringValue("22"),
      to2 = stringValue("24")
    ) should beColumns("id1", "id2").withNoRows()
    run(
      from1 = longValue(20),
      to1 = longValue(10),
      from2 = stringValue("10"),
      to2 = stringValue("20")
    ) should beColumns("id1", "id2").withNoRows()
    Seq(NO_VALUE, EMPTY_MAP).map(impossibleValue => {
      run(
        from1 = impossibleValue,
        to1 = longValue(20),
        from2 = stringValue("10"),
        to2 = stringValue("20")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from1 = longValue(10),
        to1 = impossibleValue,
        from2 = stringValue("10"),
        to2 = stringValue("20")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from1 = longValue(10),
        to1 = longValue(20),
        from2 = impossibleValue,
        to2 = stringValue("20")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from1 = longValue(10),
        to1 = longValue(20),
        from2 = stringValue("10"),
        to2 = impossibleValue
      ) should beColumns("id1", "id2").withNoRows()
    })
  }

  test("should support composite combined directed exact and range filters") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          between(gte(param("from")), lte(param("to"))),
          single(param("exact"))
        ))
      )
      .build()

    def run(from: AnyValue, to: AnyValue, exact: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "from" -> from,
            "to" -> to,
            "exact" -> exact
          )
      )

    // then
    run(
      from = longValue(10),
      to = longValue(20),
      exact = stringValue("12")
    ) should beColumns("id1", "id2").withSingleRow(12, "12")
    run(
      from = longValue(10),
      to = longValue(20),
      exact = stringValue("22")
    ) should beColumns("id1", "id2").withNoRows()
    run(
      from = longValue(20),
      to = longValue(10),
      exact = stringValue("12")
    ) should beColumns("id1", "id2").withNoRows()
    Seq(NO_VALUE, EMPTY_MAP).map(impossibleValue => {
      run(
        from = impossibleValue,
        to = longValue(20),
        exact = stringValue("10")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from = longValue(10),
        to = impossibleValue,
        exact = stringValue("10")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from = longValue(10),
        to = longValue(20),
        exact = impossibleValue
      ) should beColumns("id1", "id2").withNoRows()
    })
  }

  test("should support composite combined undirected exact and range filters") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          between(gte(param("from")), lte(param("to"))),
          single(param("exact"))
        ))
      )
      .build()

    def run(from: AnyValue, to: AnyValue, exact: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "from" -> from,
            "to" -> to,
            "exact" -> exact
          )
      )

    // then
    run(
      from = longValue(10),
      to = longValue(20),
      exact = stringValue("12")
    ) should beColumns("id1", "id2").withRows(Seq(Array(12, "12"), Array(12, "12")))
    run(
      from = longValue(10),
      to = longValue(20),
      exact = stringValue("22")
    ) should beColumns("id1", "id2").withNoRows()
    run(
      from = longValue(20),
      to = longValue(10),
      exact = stringValue("12")
    ) should beColumns("id1", "id2").withNoRows()
    Seq(NO_VALUE, EMPTY_MAP).map(impossibleValue => {
      run(
        from = impossibleValue,
        to = longValue(20),
        exact = stringValue("10")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from = longValue(10),
        to = impossibleValue,
        exact = stringValue("10")
      ) should beColumns("id1", "id2").withNoRows()
      run(
        from = longValue(10),
        to = longValue(20),
        exact = impossibleValue
      ) should beColumns("id1", "id2").withNoRows()
    })
  }

  test("directed composite queries should find relationships with missing property") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(sizeHint / 2, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
      relationshipGraph(sizeHint / 2, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          between(gte(param("from1")), lte(param("to1"))),
          between(gte(param("from2")), lte(param("to2"))),
          AllQueryExpression
        ))
      )
      .build()

    def run(from1: AnyValue, to1: AnyValue, from2: AnyValue, to2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "from1" -> from1,
            "to1" -> to1,
            "from2" -> from2,
            "to2" -> to2
          )
      )

    // then
    run(
      from1 = longValue(5),
      to1 = longValue(10),
      from2 = stringValue("5"),
      to2 = stringValue("8")
    ) should beColumns("id1", "id2", "id3").withRows(
      Seq(
        Array(5, "5", 5),
        Array(5, "5", null),
        Array(6, "6", 6),
        Array(6, "6", null),
        Array(7, "7", 7),
        Array(7, "7", null),
        Array(8, "8", 8),
        Array(8, "8", null)
      )
    )
  }

  test("undirected composite queries should find relationships with missing property") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      relationshipGraph(sizeHint / 2, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(r.getId, id3Token, byteValue(i.toByte))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
      relationshipGraph(sizeHint / 2, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          write.relationshipSetProperty(r.getId, id2Token, stringValue(i.toString))
          write.relationshipSetProperty(
            r.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("r.id1 AS id1", "r.id2 AS id2", "r.id3 AS id3")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2", "id3"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          between(gte(param("from1")), lte(param("to1"))),
          between(gte(param("from2")), lte(param("to2"))),
          AllQueryExpression
        ))
      )
      .build()

    def run(from1: AnyValue, to1: AnyValue, from2: AnyValue, to2: AnyValue) =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "from1" -> from1,
            "to1" -> to1,
            "from2" -> from2,
            "to2" -> to2
          )
      )

    // then
    run(
      from1 = longValue(5),
      to1 = longValue(10),
      from2 = stringValue("5"),
      to2 = stringValue("8")
    ) should beColumns("id1", "id2", "id3").withRows(
      Seq(
        Array(5, "5", 5),
        Array(5, "5", 5),
        Array(5, "5", null),
        Array(5, "5", null),
        Array(6, "6", 6),
        Array(6, "6", 6),
        Array(6, "6", null),
        Array(6, "6", null),
        Array(7, "7", 7),
        Array(7, "7", 7),
        Array(7, "7", null),
        Array(7, "7", null),
        Array(8, "8", 8),
        Array(8, "8", 8),
        Array(8, "8", null),
        Array(8, "8", null)
      )
    )
  }

  test("should support directed existence query") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, idToken, longValue(i))
            relationships.append(r)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(ExistenceQueryExpression)
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships))
  }

  test("should support undirected existence query") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, idToken, longValue(i))
            relationships.append(r)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(ExistenceQueryExpression)
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships.flatMap(r => Seq(r, r))))
  }

  test("should support composite directed existence query") {

    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
            write.relationshipSetProperty(r.getId, id2Token, longValue(i))
            relationships.append(r)
          } else if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          } else {
            write.relationshipSetProperty(r.getId, id2Token, longValue(i))
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(ExistenceQueryExpression, ExistenceQueryExpression))
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships))
  }

  test("should support composite undirected existence query") {

    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
            write.relationshipSetProperty(r.getId, id2Token, longValue(i))
            relationships.append(r)
          } else if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          } else {
            write.relationshipSetProperty(r.getId, id2Token, longValue(i))
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(ExistenceQueryExpression, ExistenceQueryExpression))
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships.flatMap(r => Seq(r, r))))
  }

  test("should support directed non-existence query") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, idToken, longValue(i))
          } else {
            relationships.append(r)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(NonExistenceQueryExpression)
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships))
  }

  test("should support undirected non-existence query") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, idToken, longValue(i))
          } else {
            relationships.append(r)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(NonExistenceQueryExpression)
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships.flatMap(r => Seq(r, r))))
  }

  test("should support composite directed non-existence query") {

    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
            write.relationshipSetProperty(r.getId, id2Token, longValue(i))
          } else if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          } else {
            relationships.append(r)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(NonExistenceQueryExpression, NonExistenceQueryExpression))
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships))
  }

  test("should support composite undirected non-existence query") {

    // given
    val relationships = ArrayBuffer.empty[Relationship]
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(1000, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
            write.relationshipSetProperty(r.getId, id2Token, longValue(i))
          } else if (random.nextBoolean()) {
            write.relationshipSetProperty(r.getId, id1Token, longValue(i))
          } else {
            relationships.append(r)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipVectorIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(NonExistenceQueryExpression, NonExistenceQueryExpression))
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    ) should beColumns("r").withRows(singleColumn(relationships.flatMap(r => Seq(r, r))))
  }

  test("should work without issues on the RHS of apply") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    val size = 10
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(size, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          relationships.append(r)
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "vector",
        limit = s"10000000",
        argumentIds = Set("vector")
      )
      .input(variables = Seq("vector"))
      .build()

    // then
    val input = inputValues((1 to size).map(_ => Array[Any](randomVector)): _*)
    val expected = relationships.flatMap(r => (1 to size).map(_ => Array(r)))
    execute(logicalQuery, runtime, input) should beColumns("r").withRows(expected)
  }

  test("should work without issues on the RHS of cartesian product") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    val size = 10
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(size, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
          relationships.append(r)
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "r")
      .cartesianProduct()
      .|.relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = vectorAsCypherList(randomVector),
        limit = s"10000000"
      )
      .input(variables = Seq("i"))
      .build()

    // then
    val input = inputValues((1 to size).map(i => Array[Any](i)): _*)
    val expected = relationships.flatMap(n => (1 to size).map(i => Array(i, n)))
    execute(logicalQuery, runtime, input) should beColumns("i", "r").withRows(expected)
  }

  test("sort on top of vector search") {
    // given
    val relationships = ArrayBuffer.empty[Relationship]
    val size = 10
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(size, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.relationshipSetProperty(n.getId, vectorToken, randomVector)
          write.relationshipSetProperty(n.getId, idToken, longValue(i))
          relationships.append(n)
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .sort("r DESC")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = vectorAsCypherList(randomVector),
        limit = s"10000000",
        score = "score"
      )
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns("r").withRows(inOrder(relationships.sortBy(-_.getId).map(Array(_))))
  }

  test("should work with durations") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          write.relationshipSetProperty(r.getId, idToken, Values.durationValue(Duration.ofSeconds(i)))
      })
    }
    // when

    val d1 = Duration.ofSeconds(10)
    val d2 = Duration.ofSeconds(20)
    def executeDurationQuery(predicate: QueryExpression[Expression]) = {
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("dur")
        .projection("r.id AS dur")
        .relationshipVectorIndexSearch(
          "()-[r]->()",
          typeNames = Seq("Foo"),
          properties = Seq("v", "id"),
          indexName = "VectorIndex",
          vector = vectorAsCypherList(randomVector),
          limit = s"10000000",
          score = "score",
          propertyFilter = Some(predicate)
        )
        .build()

      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "d1" -> d1,
            "d2" -> d2
          )
      )
    }

    // then
    executeDurationQuery(single(param("d1"))) should beColumns("dur").withSingleRow(d1)
    executeDurationQuery(single(param("d2"))) should beColumns("dur").withSingleRow(d2)
    executeDurationQuery(rangeExpression(gte(param("d1")))) should beColumns("dur").withSingleRow(d1)
    executeDurationQuery(rangeExpression(gt(param("d1")))) should beColumns("dur").withNoRows()
    executeDurationQuery(rangeExpression(lte(param("d1")))) should beColumns("dur").withSingleRow(d1)
    executeDurationQuery(rangeExpression(lt(param("d1")))) should beColumns("dur").withNoRows()

    executeDurationQuery(between(gt(param("d1")), lt(param("d2")))) should beColumns("dur").withNoRows()
    executeDurationQuery(between(gte(param("d1")), lt(param("d2")))) should beColumns("dur").withNoRows()
    executeDurationQuery(between(gt(param("d1")), lte(param("d2")))) should beColumns("dur").withNoRows()
    executeDurationQuery(between(gte(param("d1")), lte(param("d2")))) should beColumns("dur").withNoRows()

    executeDurationQuery(between(gt(param("d1")), lt(param("d1")))) should beColumns("dur").withNoRows()
    executeDurationQuery(between(gte(param("d1")), lt(param("d1")))) should beColumns("dur").withNoRows()
    executeDurationQuery(between(gt(param("d1")), lte(param("d1")))) should beColumns("dur").withNoRows()
    executeDurationQuery(between(gte(param("d1")), lte(param("d1")))) should beColumns("dur").withSingleRow(d1)
  }

  // entity filtering
  test("should filter entities") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("v")
      val idToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .apply()
      .|.relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(randomVector)}",
        limit = "13",
        entityFilter = matchEntities(varFor("c")),
        argumentIds = Set("c")
      )
      .aggregation(Map.empty[String, Expression], Map("c" -> collectDistinctIds(id(varFor("x")))))
      .filter("x.id < 100")
      .allRelationshipsScan("()-[x]->()")
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns("id").withRows(matching {
      case rows: Seq[_] if rows.size == 13 && rows.forall {
          case Array(i: NumberValue) => i.longValue() >= 0 && i.longValue() < 100
          case _                     => false
        } =>
    })
  }

  test("should filter entities and do property filtering") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("v")
      val idToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .apply()
      .|.relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(randomVector)}",
        limit = "13",
        entityFilter = matchEntities(varFor("c")),
        propertyFilter = Some(rangeExpression(lt(literalInt(17)))),
        argumentIds = Set("c")
      )
      .aggregation(Map.empty[String, Expression], Map("c" -> collectDistinctIds(id(varFor("x")))))
      .filter("x.id < 100")
      .allRelationshipsScan("()-[x]->()")
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns("id").withRows(matching {
      case rows: Seq[_] if rows.size == 13 && rows.forall {
          case Array(i: NumberValue) => i.longValue() >= 0 && i.longValue() < 17
          case _                     => false
        } =>
    })
  }

  test("should fail if entities isn't a list of ids") {
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("v")
      val idToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (r, i) =>
          write.relationshipSetProperty(r.getId, vectorToken, randomVector)
          write.relationshipSetProperty(r.getId, idToken, longValue(i))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .apply()
      .|.relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(randomVector)}",
        limit = "13",
        entityFilter = matchEntities(varFor("c")),
        argumentIds = Set("c")
      )
      .aggregation(Seq.empty, Seq("collect('hello') AS c"))
      .filter("x.id < 100")
      .allRelationshipsScan("()-[x]->()")
      .build()

    // then
    the[Neo4jException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22G03,
      "error: data exception - invalid value type"
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N01,
      "error: data exception - invalid type.",
      fuzzyStatusDescr = true
    )
  }

  // IN/OR queries
  test("simple OR/IN query") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.relationshipSetProperty(n.getId, idToken, longValue(i))
          write.relationshipSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(many(listOfInt(0, sizeHint / 2, sizeHint - 1, sizeHint)))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector
          )
      )

    // then
    runtimeResult should beColumns("id").withRows(singleColumn(Seq(0, sizeHint / 2, sizeHint - 1)))
  }

  test("composite OR/IN query") {
    // given
    givenGraph {
      relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.relationshipSetProperty(n.getId, vectorToken, randomVector)
          write.relationshipSetProperty(n.getId, id1Token, longValue(i))
          write.relationshipSetProperty(n.getId, id2Token, longValue(i))
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("r.id1 AS id1", "r.id2 AS id2")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "id1", "id2"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(composite(
          many(listOfInt(0, sizeHint / 2, sizeHint - 1, sizeHint)),
          many(listOfInt(sizeHint / 2, sizeHint))
        ))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector
          )
      )

    // then
    runtimeResult should beColumns("id1", "id2").withSingleRow(sizeHint / 2, sizeHint / 2)
  }

  private def booleanVectorGraph(size: Int): Unit = {
    relationshipIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "bool")
    val write = tx.kernelTransaction().dataWrite
    val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
    val boolToken = tx.kernelTransaction().tokenRead().propertyKey("bool")
    relationshipGraph(size, "Foo").zipWithIndex.foreach({
      case (r, i) =>
        write.relationshipSetProperty(r.getId, boolToken, Values.booleanValue(i < size / 2))
        write.relationshipSetProperty(
          r.getId,
          vectorToken,
          randomVector
        )
    })
  }

  private def executeBooleanPlan(
    rangePredicate: RangeQueryExpression[InequalitySeekRangeWrapper],
    min: Boolean,
    max: Boolean = true
  ) = {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("result")
      .projection("r.bool AS result")
      .relationshipVectorIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("v", "bool"),
        indexName = "VectorIndex",
        vector = vectorAsCypherList(randomVector),
        limit = "10000",
        propertyFilter = Some(rangePredicate)
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "min" -> Values.booleanValue(min),
          "max" -> Values.booleanValue(max)
        )
    )
  }

  private def param(name: String) = parameter(name, CTAny)

  private def vectorAsCypherList(v: VectorValue): String = {
    var i = 0
    val builder = new StringBuilder
    builder.append("[")
    while (i < v.dimensions()) {
      builder.append(v.doubleValue(i))
      if (i < v.dimensions() - 1) {
        builder.append(", ")
      }
      i += 1
    }
    builder.append("]")
    builder.toString()
  }
}

object RelationshipVectorIndexSearchTestBase {}
