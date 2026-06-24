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
import org.neo4j.cypher.internal.runtime.spec.DriverGqlStatusAdapter.asGqlException
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.GqlExceptionMatcher
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.test.TestDatabaseManagementServiceFactorySupplier
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueType
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.booleanValue
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
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import java.time.Duration
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneOffset

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.comparatorToOrdering
import scala.reflect.ClassTag
import scala.util.Random

//noinspection ScalaDeprecation,RedundantDefaultArgument
abstract class NodeVectorIndexSearchTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition,
      runtime
    ) with QueryExpressionConstructionTestSupport {

  private val configurations: Seq[(String, VectorValue)] = Seq(
    ("INT64", int64Vector(1L to sizeHint: _*)),
    ("INT32", int32Vector(1 to sizeHint: _*)),
    ("INT16", int16Vector((1 to sizeHint).map(_.toShort): _*)),
    ("INT8", int8Vector((1 to sizeHint).map(_.toByte): _*)),
    ("FLOAT64", float64Vector((1 to sizeHint).map(_.toDouble): _*)),
    ("FLOAT32", float32Vector((1 to sizeHint).map(_.toFloat): _*))
  )

  configurations.foreach {
    case (name, vector) =>
      test(s"$name vector index should index vector values with score variable") {
        givenGraph {
          nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          nodeGraph(1, "Foo").foreach(n => {
            n.setProperty("id", 1)
            n.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id", "score")
          .projection("n.id AS id")
          .nodeVectorIndexSearch(
            node = "n",
            labelNames = Seq("Foo"),
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

      test(s"$name vector index should index vector values without score variable") {
        givenGraph {
          nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          nodeGraph(1, "Foo").foreach(n => {
            n.setProperty("id", 1)
            n.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("n.id AS id")
          .nodeVectorIndexSearch(
            node = "n",
            labelNames = Seq("Foo"),
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

      test(
        s"$name should be able to search using a list of integers instead of an explicit vector"
      ) {
        givenGraph {
          nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
          nodeGraph(1, "Foo").foreach(n => {
            n.setProperty("id", 1)
            n.setProperty("v", vector)
          })
        }

        // when
        val logicalQuery = new LogicalQueryBuilder(this)
          .produceResults("id", "score")
          .projection("n.id AS id")
          .nodeVectorIndexSearch(
            node = "n",
            labelNames = Seq("Foo"),
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
  }

  test("should be able to query the index with multiple inputs from a property") {
    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      })
    }

    // when
    val limit = 11
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .apply()
      .|.nodeVectorIndexSearch(
        node = "m",
        labelNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "n.v",
        limit = s"$limit",
        score = "score",
        argumentIds = Set("n")
      )
      .filter("n.id < 20")
      .nodeByLabelScan("n", "Foo")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id", "score").withRows(rowCount(20 * limit))
  }

  test("should fail if index doesn't exists") {
    val theVector = float32Vector((1 to sizeHint).map(_.toFloat): _*)
    givenGraph {
      nodeGraph(1, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", theVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      nodeIndex("VectorIndex", IndexType.RANGE, Seq("Foo"), "v")
      nodeGraph(1, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", "theVector")
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(1, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", theVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(1, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", theVector)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(1, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", int32Vector(1, 2, 3))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should respect the limit") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13",
        score = "score"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*)))
    runtimeResult should beColumns("id", "score").withRows(rowCount(13))
  }

  test("should handle limit 0") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "0",
        score = "score"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*)))
    runtimeResult should beColumns("id", "score").withNoRows()
  }

  test("should fail on negative limits") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should fail on too large limits") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*))
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value 9223372036854775807 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'LIMIT' to be of type INTEGER NOT NULL and in the range 0 to 2147483647 but found 9223372036854775807."
    )
  }

  test("should support multiple labels (on same node)") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo", "Bar", "Baz"), "v")
      nodeGraph(sizeHint, "Foo", "Bar", "Baz").zipWithIndex.foreach({
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*)))
    runtimeResult should beColumns("labels").withRows(
      Seq.fill(13)(Array(Array("Foo", "Bar", "Baz"))),
      listInAnyOrder = true
    )
  }

  test("should support multiple labels (on different nodes)") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo", "Bar", "Baz"), "v")
      nodeGraph(1, "Foo").foreach(n =>
        n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      )
      nodeGraph(1, "Bar").foreach(n =>
        n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      )
      nodeGraph(1, "Baz").foreach(n =>
        n.setProperty("v", float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*))
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = "13"
      ).build()

    val runtimeResult =
      execute(logicalQuery, runtime, parameters = Map("vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*)))
    runtimeResult should beColumns("labels").withRows(Seq(
      Array(Array("Foo")),
      Array(Array("Bar")),
      Array(Array("Baz"))
    ))
  }

  // TODO: arrays and duration not supported yet
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
    val random = new Random()
    // we use a size small enough to have unique sequence of byte values
    val size = 256

    def createGraph(): Unit = {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(size, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, range(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*)
          )
      })
    }

    val indexToSearch = random.nextInt(size)
    val valueToSearch = range(indexToSearch)

    test(s"should support single-stage filtering single exact seek with  n.id = $valueToSearch") {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withSingleRow(valueToSearch)
    }

    test(
      s"should support single-stage filtering single range seek $valueToSearch <= n.id <= $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withSingleRow(valueToSearch)
    }

    test(
      s"should support single-stage filtering single range seek $valueToSearch < n.id <= $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single range seek $valueToSearch <= n.id < $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single range seek $valueToSearch < n.id < $valueToSearch"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single range seek ${range(size - 1)} <= n.id <= ${range(0)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "min" -> range(size - 1),
              "max" -> range(0)
            )
        )

      // then
      runtimeResult should beColumns("id").withNoRows()
    }

    test(
      s"should support single-stage filtering single range seek ${range(0)} <= n.id < ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (0 until size - 1).map(range)
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single range seek ${range(0)} < n.id <= ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (1 until size).map(range)
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(
      s"should support single-stage filtering single range seek ${range(0)} <= n.id <= ${range(size - 1)}"
    ) {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("id")
        .projection("n.id AS id")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "min" -> range(0),
              "max" -> range(size - 1)
            )
        )

      // then
      val expected = (0 until size).map(range)
      runtimeResult should beColumns("id").withRows(singleColumn(expected))
    }

    test(s"should support single-stage filtering single range seek $valueToSearch < n.id") {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .projection("n.id > $seekValue AS r")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )
      }

      // then
      val expectedNumberOfRows = size - indexToSearch - 1
      runtimeResult should beColumns("r").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(s"should support single-stage filtering single range seek $valueToSearch <= n.id") {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .projection("n.id >= $seekValue AS r")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = size - indexToSearch
      runtimeResult should beColumns("r").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(s"should support single-stage filtering single range seek $valueToSearch > n.id") {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .projection("n.id < $seekValue AS r")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = indexToSearch
      runtimeResult should beColumns("r").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }

    test(s"should support single-stage filtering single range seek $valueToSearch >= n.id") {
      // given
      givenGraph(createGraph())

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .projection("n.id <= $seekValue AS r")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
              "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
              "seekValue" -> valueToSearch
            )
        )

      // then
      val expectedNumberOfRows = indexToSearch + 1
      runtimeResult should beColumns("r").withRows(singleColumn(Seq.fill(expectedNumberOfRows)(true)))
    }
  })

  test("should support single-stage filtering single range seek with different types") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*)
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
            "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
            "seekValue1" -> longValue(0),
            "seekValue2" -> stringValue("10000")
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test(
    "should support single-stage filtering single range seek with different non-storable types"
  ) {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*)
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
            "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
            "seekValue1" -> longValue(0),
            "seekValue2" -> EMPTY_MAP
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test("should support single-stage filtering single open range seek with non-storable type") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*)
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
            "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
            "seekValue" -> EMPTY_MAP
          )
      )
    runtimeResult should beColumns("id").withNoRows()
  }

  test("should support single-stage filtering single range seek between null values") {

    val random = new Random()

    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*)
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
            "vector" -> float32Vector(Seq.fill(sizeHint)(5.0f): _*),
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
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = true,
      max = true
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = true,
      max = false
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lt(param("max"))),
      min = false,
      max = false
    ) should beColumns("r").withNoRows()
    // [FALSE, TRUE] <= n.bool < [FALSE, TRUE]
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = false,
      max = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false)))
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = true,
      max = true
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = true,
      max = false
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gte(param("min")), lt(param("max"))),
      min = false,
      max = false
    ) should beColumns("r").withNoRows()
    // [FALSE, TRUE] < n.bool <= [FALSE, TRUE]
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = false,
      max = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(true)))
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = true,
      max = true
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = true,
      max = false
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gt(param("min")), lte(param("max"))),
      min = false,
      max = false
    ) should beColumns("r").withNoRows()
    // [FALSE, TRUE] <= n.bool <= [FALSE, TRUE]
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = false,
      max = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false) ++ Seq.fill(10)(true)))
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = true,
      max = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(true)))
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = true,
      max = false
    ) should beColumns("r").withNoRows()
    executeBooleanPlan(
      between(gte(param("min")), lte(param("max"))),
      min = false,
      max = false
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false)))

    // [FALSE, TRUE] <= n.bool
    executeBooleanPlan(
      rangeExpression(gte(param("min"))),
      min = false
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false) ++ Seq.fill(10)(true)))
    executeBooleanPlan(
      rangeExpression(gte(param("min"))),
      min = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(true)))
    // [FALSE, TRUE] < n.bool
    executeBooleanPlan(
      rangeExpression(gt(param("min"))),
      min = false
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(true)))
    executeBooleanPlan(rangeExpression(gt(param("min"))), min = true) should beColumns("r").withNoRows()
    // [FALSE, TRUE] >= n.bool
    executeBooleanPlan(
      rangeExpression(lte(param("min"))),
      min = false
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false)))
    executeBooleanPlan(
      rangeExpression(lte(param("min"))),
      min = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false) ++ Seq.fill(10)(true)))
    // [FALSE, TRUE] > n.bool
    executeBooleanPlan(rangeExpression(lt(param("min"))), min = false) should beColumns("r").withNoRows()
    executeBooleanPlan(
      rangeExpression(lt(param("min"))),
      min = true
    ) should beColumns("r").withRows(singleColumn(Seq.fill(10)(false)))
  }

  /**
   * Tests so that we get equivalent results from a normal range index query
   * compared to what we get from a filtering vector stage query.
   */
  private val seed: Long = System.currentTimeMillis()
  println("seed=$seed")
  private val random = RandomValues.create(new java.util.Random(seed))
  private def randomVector = random.nextFloat32Vector(1536, 1536)

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
    def randomSearchPredicate: (QueryExpression[Expression], String) = {
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      nodeIndex("RangeIndex", IndexType.RANGE, Seq("Foo"), "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").foreach({
        n =>
          write.nodeSetProperty(n.getId, idToken, randomValue)
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    test(s"index equivalence test iteration=$i") {
      withClue(s"seed=$seed predicate=$predicateString") {
        // given
        givenGraph(randomGraph())

        // when
        val vectorQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("n.id AS id")
          .nodeVectorIndexSearch(
            node = "n",
            labelNames = Seq("Foo"),
            properties = Seq("v", "id"),
            indexName = "VectorIndex",
            vector = "$vector",
            limit = s"10000000",
            propertyFilter = Some(predicate)
          )
          .build()

        val rangeQuery = new LogicalQueryBuilder(this)
          .produceResults("id")
          .projection("n.id AS id")
          .nodeIndexOperator("n:Foo(id)", customQueryExpression = Some(predicate))
          .build()

        // then
        run(vectorQuery) should contain theSameElementsAs run(rangeQuery)
      }
    }
  })

  test("comparing floating point and integer") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(1, "Foo").foreach({
        n =>
          write.nodeSetProperty(n.getId, idToken, longValue(1))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(1, "Foo").foreach({
        n =>
          write.nodeSetProperty(n.getId, idToken, longValue(1))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support multiple composite exact filters") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    val toFind = random.nextInt(sizeHint)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("n.id1 AS id1", "n.id2 AS id2")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support exact composite query with one gap") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(n.getId, id3Token, byteValue(i.toByte))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("n.id1 AS id1", "n.id2 AS id2", "n.id3 AS id3")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support exact composite query with two gaps") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(n.getId, id3Token, byteValue(i.toByte))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("n.id1 AS id1", "n.id2 AS id2", "n.id3 AS id3")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support exact composite query with just gaps") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(n.getId, id3Token, byteValue(i.toByte))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("n.id1 AS id1", "n.id2 AS id2", "n.id3 AS id3")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support multiple composite range filters") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("n.id1 AS id1", "n.id2 AS id2")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support composite combined exact and range filters") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("n.id1 AS id1", "n.id2 AS id2")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("composite queries should find nodes with missing property") {

    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2", "id3")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      val id3Token = tx.kernelTransaction().tokenRead().propertyKey("id3")
      nodeGraph(sizeHint / 2, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(n.getId, id3Token, byteValue(i.toByte))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
      nodeGraph(sizeHint / 2, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, stringValue(i.toString))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2", "id3")
      .projection("n.id1 AS id1", "n.id2 AS id2", "n.id3 AS id3")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("should support existence query") {

    // given
    val nodes = ArrayBuffer.empty[Node]
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.nodeSetProperty(n.getId, idToken, longValue(i))
            nodes.append(n)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
    ) should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should support composite existence query") {

    // given
    val nodes = ArrayBuffer.empty[Node]
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.nodeSetProperty(n.getId, id1Token, longValue(i))
            write.nodeSetProperty(n.getId, id2Token, longValue(i))
            nodes.append(n)
          } else if (random.nextBoolean()) {
            write.nodeSetProperty(n.getId, id1Token, longValue(i))
          } else {
            write.nodeSetProperty(n.getId, id2Token, longValue(i))
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
    ) should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should support non-existence query") {

    // given
    val nodes = ArrayBuffer.empty[Node]
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.nodeSetProperty(n.getId, idToken, longValue(i))
          } else {
            nodes.append(n)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
    ) should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should support composite non-existence query") {

    // given
    val nodes = ArrayBuffer.empty[Node]
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      nodeGraph(1000, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          if (random.nextBoolean()) {
            write.nodeSetProperty(n.getId, id1Token, longValue(i))
            write.nodeSetProperty(n.getId, id2Token, longValue(i))
          } else if (random.nextBoolean()) {
            write.nodeSetProperty(n.getId, id1Token, longValue(i))
          } else {
            nodes.append(n)
          }
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
    ) should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should work without issues on the RHS of apply") {
    // given
    val nodes = ArrayBuffer.empty[Node]
    val size = 10
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(size, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          nodes.append(n)
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
    val expected = nodes.flatMap(n => (1 to size).map(_ => Array(n)))
    execute(logicalQuery, runtime, input) should beColumns("n").withRows(expected)
  }

  test("should work without issues on the RHS of cartesian product") {
    // given
    val nodes = ArrayBuffer.empty[Node]
    val size = 10
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(size, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          nodes.append(n)
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "n")
      .cartesianProduct()
      .|.nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = vectorAsCypherList(randomVector),
        limit = s"10000000"
      )
      .input(variables = Seq("i"))
      .build()

    // then
    val input = inputValues((1 to size).map(i => Array[Any](i)): _*)
    val expected = nodes.flatMap(n => (1 to size).map(i => Array(i, n)))
    execute(logicalQuery, runtime, input) should beColumns("i", "n").withRows(expected)
  }

  test("sort on top of vector search") {
    // given
    val nodes = ArrayBuffer.empty[Node]
    val size = 10
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(size, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          nodes.append(n)
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .sort("n DESC")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = vectorAsCypherList(randomVector),
        limit = s"10000000",
        score = "score"
      )
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns("n").withRows(inOrder(nodes.sortBy(-_.getId).map(Array(_))))
  }

  test("should work with durations") {
    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          write.nodeSetProperty(n.getId, idToken, Values.durationValue(Duration.ofSeconds(i)))
      })
    }
    // when
    val d1 = Duration.ofSeconds(10)
    val d2 = Duration.ofSeconds(20)
    def executeDurationQuery(predicate: QueryExpression[Expression]) = {
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("dur")
        .projection("n.id AS dur")
        .nodeVectorIndexSearch(
          node = "n",
          labelNames = Seq("Foo"),
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
    executeDurationQuery(rangeExpression(gte(param("d1")))) should beColumns("dur").withSingleRow(d1)
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

  test("should fail if filter type isn't supported") {
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(11, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, Values.longArray(Array(42L)))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(randomVector)}",
        limit = "13",
        score = "score",
        propertyFilter = Some(single(listOf(literalInt(42))))
      ).build()

    // then
    assertGqlError[InvalidArgumentException](
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22G03,
        "error: data exception - invalid value type"
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N01,
        "error: data exception - invalid type. Expected the value [42] to be of type INTEGER, FLOAT, STRING, BOOLEAN, DATE, LOCAL TIME, ZONED TIME, LOCAL DATETIME, ZONED DATETIME or DURATION, but was of type LIST<INTEGER>."
      )
    ) {
      consume(execute(logicalQuery, runtime))
    }
  }

  // entity filtering
  test("should filter entities") {
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("v")
      val idToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, Values.longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .apply()
      .|.nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(randomVector)}",
        limit = "13",
        entityFilter = matchEntities(varFor("c")),
        argumentIds = Set("c")
      )
      .aggregation(Map.empty[String, Expression], Map("c" -> collectDistinctIds(id(varFor("x")))))
      .filter("x.id < 100")
      .allNodeScan("x")
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("v")
      val idToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, Values.longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .apply()
      .|.nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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
      .allNodeScan("x")
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("v")
      val idToken = tx.kernelTransaction().token().propertyKeyGetOrCreateForName("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, Values.longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .apply()
      .|.nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v"),
        indexName = "VectorIndex",
        vector = s"${vectorAsCypherList(randomVector)}",
        limit = "13",
        entityFilter = matchEntities(varFor("c")),
        argumentIds = Set("c")
      )
      .aggregation(Seq.empty, Seq("collect('hello') AS c"))
      .filter("x.id < 100")
      .allNodeScan("x")
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
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("IN query with empty predicate with empty list") {
    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(many(listOfInt()))
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
    runtimeResult should beColumns("id").withNoRows()
  }

  test("IN query with empty predicate with non list") {
    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, longValue(i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(many(literalInt(2)))
      )
      .build()

    // then
    the[CypherTypeException] thrownBy consume(execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "vector" -> randomVector
        )
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22G03,
      "error: data exception - invalid value type"
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N01,
      "error: data exception - invalid type. Expected the value 2 to be of type LIST, but was of type INTEGER NOT NULL."
    )
  }

  test("composite OR/IN query") {
    // given
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id1", "id2")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val id1Token = tx.kernelTransaction().tokenRead().propertyKey("id1")
      val id2Token = tx.kernelTransaction().tokenRead().propertyKey("id2")
      nodeGraph(sizeHint, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, vectorToken, randomVector)
          write.nodeSetProperty(n.getId, id1Token, longValue(i))
          write.nodeSetProperty(n.getId, id2Token, longValue(i))
      })
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id1", "id2")
      .projection("n.id1 AS id1", "n.id2 AS id2")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
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

  test("IN query with many, but not too many items") {
    // given
    val totalSize = 5000
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(totalSize, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, DurationValue.duration(i, i, i, i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val propFilter = (1L to 256).map(i => DurationValue.duration(i, i, i, i))
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(many(param("list")))
      )
      .build()

    val runtimeResult =
      execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "list" -> VirtualValues.list(propFilter: _*)
          )
      )

    // then
    runtimeResult should beColumns("id").withRows(singleColumn(propFilter))
  }

  test("IN query with too many items") {
    // given
    val totalSize = 5000
    givenGraph {
      nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "id")
      val write = tx.kernelTransaction().dataWrite
      val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
      val idToken = tx.kernelTransaction().tokenRead().propertyKey("id")
      nodeGraph(totalSize, "Foo").zipWithIndex.foreach({
        case (n, i) =>
          write.nodeSetProperty(n.getId, idToken, DurationValue.duration(i, i, i, i))
          write.nodeSetProperty(
            n.getId,
            vectorToken,
            randomVector
          )
      })
    }

    // when
    val propFilter = (1L to 257).map(i => DurationValue.duration(i, i, i, i))
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "id"),
        indexName = "VectorIndex",
        vector = "$vector",
        limit = s"10000000",
        propertyFilter = Some(many(param("list")))
      )
      .build()

    // then
    assertGqlError[InvalidArgumentException](
      gqlStatus(
        GqlStatusInfoCodes.STATUS_22003,
        "error: data exception - numeric value out of range. The numeric value 1028 is outside the required range."
      ).withCause(
        GqlStatusInfoCodes.STATUS_22N03,
        "error: data exception - specified numeric value out of range. Expected 'size-of-predicate-list' to be of type INTEGER NOT NULL and in the range 0 to 1024 but found 1028.",
        fuzzyStatusDescr = true
      )
    ) {
      consume(execute(
        logicalQuery,
        runtime,
        parameters =
          Map(
            "vector" -> randomVector,
            "list" -> VirtualValues.list(propFilter: _*)
          )
      ))
    }
  }

  // Off-SPD asserts the concrete exception type T; under SPD the error crosses the bolt
  // boundary as a driver type, so we match the GQL status anywhere in the cause chain.
  private def assertGqlError[T <: Throwable with ErrorGqlStatusObject : ClassTag](
    matcher: GqlExceptionMatcher
  )(block: => Unit): Unit =
    if (TestDatabaseManagementServiceFactorySupplier.isSpd) {
      the[Throwable] thrownBy block shouldBe asGqlException(matcher)
    } else {
      the[T] thrownBy block shouldBe matcher
    }

  private def booleanVectorGraph(size: Int): Unit = {
    val random = new Random()
    nodeIndex("VectorIndex", IndexType.VECTOR, Seq("Foo"), "v", "bool")
    val write = tx.kernelTransaction().dataWrite
    val vectorToken = tx.kernelTransaction().tokenRead().propertyKey("v")
    val boolToken = tx.kernelTransaction().tokenRead().propertyKey("bool")
    nodeGraph(size, "Foo").zipWithIndex.foreach({
      case (n, i) =>
        write.nodeSetProperty(n.getId, boolToken, booleanValue(i < size / 2))
        write.nodeSetProperty(
          n.getId,
          vectorToken,
          float32Vector((1 to sizeHint).map(_ => random.between(0f, 10f)): _*)
        )
    })
  }

  private def executeBooleanPlan(
    rangePredicate: RangeQueryExpression[InequalitySeekRangeWrapper],
    min: Boolean,
    max: Boolean = true
  ) = {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .projection("n.bool AS r")
      .nodeVectorIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("v", "bool"),
        indexName = "VectorIndex",
        vector = vectorAsCypherList(float32Vector(Seq.fill(sizeHint)(5.0f): _*)),
        limit = "10000",
        propertyFilter = Some(rangePredicate)
      )
      .build()

    execute(
      logicalQuery,
      runtime,
      parameters =
        Map(
          "min" -> booleanValue(min),
          "max" -> booleanValue(max)
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

object NodeVectorIndexSearchTestBase {}
