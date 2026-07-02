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
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values.longValue

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

abstract class NodeFulltextIndexSearchTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition,
      runtime
    ) {

  test("should find matching nodes with score variable") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(n => {
        n.setProperty("id", 1)
        n.setProperty("prop", "the cat sat on the mat")
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("n.id AS id")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id", "score").withRows(matching {
      case Seq(Array(id, score: NumberValue)) if id == longValue(1) && score.doubleValue() > 0.0 =>
    })
  }

  test("should find matching nodes without score variable") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(n => {
        n.setProperty("id", 1)
        n.setProperty("prop", "the cat sat on the mat")
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id").withSingleRow(1)
  }

  test("should only find matching nodes") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(sizeHint, "Doc").zipWithIndex.foreach {
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("prop", if (i % 2 == 0) "the cat sat on the mat" else "the dog barked")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$sizeHint"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = (0 until sizeHint).filter(_ % 2 == 0)
    runtimeResult should beColumns("id").withRows(singleColumn(expected))
  }

  test("should return results in descending score order") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(sizeHint, "Doc").zipWithIndex.foreach {
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("prop", "cat" + " filler" * (i % 10))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$sizeHint",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("score").withRows(sortedDesc("score"))
  }

  test("should respect the limit") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(sizeHint, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    val limit = math.min(13, sizeHint)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$limit",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "score").withRows(rowCount(limit))
  }

  test("should handle limit 0") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(sizeHint, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "0",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "score").withNoRows()
  }

  test("should fail on negative limit") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "-1",
        score = "score"
      )
      .build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value -1 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'value' to be of type INTEGER and in the range 0 to 9223372036854775807 but found -1."
    )
  }

  test("should respect skip") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(sizeHint, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val skip = sizeHint / 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$sizeHint",
        skip = Some(s"$skip"),
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "score").withRows(rowCount(sizeHint - skip))
  }

  test("should respect skip combined with limit") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(sizeHint, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val skip = 5
    val limit = 7
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$limit",
        skip = Some(s"$skip"),
        score = "score"
      )
      .build()

    // then
    val expected = math.max(0, math.min(limit, sizeHint - skip))
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "score").withRows(rowCount(expected))
  }

  test("should skip the highest scoring matches") {
    // each node gets a distinct score: more filler words => lower score
    val n = math.min(sizeHint, 50)
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(n, "Doc").zipWithIndex.foreach {
        case (node, i) => node.setProperty("prop", "cat" + " filler" * i)
      }
    }

    def query(skip: Option[String]) = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$n",
        skip = skip,
        score = "score"
      )
      .build()

    // baseline: the full result, in descending score order
    val ordered = consume(execute(query(skip = None), runtime))
    val skip = n / 2

    // then: skipping drops the top `skip` highest-scoring rows, not arbitrary rows
    execute(query(skip = Some(s"$skip")), runtime) should
      beColumns("n", "score").withRows(inOrder(ordered.drop(skip)))
  }

  test("should support analyzer override") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "Hello world"))
    }

    def query(analyzer: Option[String]) = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'Hello'",
        limit = "20",
        analyzer = analyzer
      )
      .build()

    // the default analyzer lowercases terms at index time; a case-sensitive
    // query-time analyzer therefore cannot match the capitalized query token
    execute(query(analyzer = None), runtime) should beColumns("n").withRows(rowCount(1))
    execute(query(analyzer = Some("'whitespace'")), runtime) should beColumns("n").withNoRows()
  }

  test("should fall back to the default analyzer when the analyzer expression is null") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "Hello world"))
    }

    // a null analyzer means "no override": it must behave exactly like analyzer = None and fall back to the index
    // default analyzer (which lowercases and so matches the capitalized query token), unlike the case-sensitive
    // 'whitespace' analyzer which would not match
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'Hello'",
        limit = "20",
        analyzer = Some("NULL")
      )
      .build()

    execute(logicalQuery, runtime) should beColumns("n").withRows(rowCount(1))
  }

  test("should support a per-argument analyzer expression on the RHS of an Apply") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "Hello world"))
    }

    // the analyzer is supplied per input row as an argument: a null argument falls back to the default analyzer
    // (matching the Doc), while the case-sensitive 'whitespace' analyzer does not match the capitalized token
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'Hello'",
        limit = "20",
        analyzer = Some("analyzerArg"),
        argumentIds = Set("analyzerArg")
      )
      .input(variables = Seq("analyzerArg"))
      .build()

    // null analyzer -> default analyzer -> 1 match; 'whitespace' -> 0 matches => 1 row total
    val input = inputValues(Array[Any](null), Array[Any]("whitespace"))
    execute(logicalQuery, runtime, input) should beColumns("n").withRows(rowCount(1))
  }

  test("should be able to query the index with multiple inputs from a property") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      Seq("cat", "dog", "cat dog").zip(nodeGraph(3, "Doc")).foreach {
        case (text, n) => n.setProperty("prop", text)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("m", "score")
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "m",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "n.prop",
        limit = "20",
        score = "score",
        argumentIds = Set("n")
      )
      .nodeByLabelScan("n", "Doc")
      .build()

    // then: 'cat' matches 2 docs, 'dog' matches 2 docs, 'cat dog' matches all 3
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("m", "score").withRows(rowCount(7))
  }

  test("should return empty if query string is null") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "NULL",
        limit = "20",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n", "score").withNoRows()
  }

  test("should return empty for a null query string even with a negative limit") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // a null queryString must short-circuit to empty before the limit is validated, so a negative limit must not
    // throw — consistent across interpreted, slotted, pipelined and parallel
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "NULL",
        limit = "-1"
      )
      .build()

    execute(logicalQuery, runtime) should beColumns("n").withNoRows()
  }

  test("should fail if query string has the wrong type") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "42",
        limit = "20",
        score = "score"
      )
      .build()

    // then
    a[CypherTypeException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("should fail if index doesn't exist") {
    givenGraph {
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20",
        score = "score"
      )
      .build()

    // then
    the[IndexNotFoundKernelException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22N69,
      "error: data exception - index does not exist. The index 'FulltextIndex' does not exist."
    )
  }

  test("should fail if index isn't a fulltext index") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.RANGE, Seq("Doc"), "prop")
      nodeGraph(1, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "score")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20",
        score = "score"
      )
      .build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime
    )) should have message "22NCG: Expected the index `FulltextIndex` to be a fulltext index but was a range index."
  }

  test("should support multiple labels (on same node)") {

    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo", "Bar", "Baz"), "prop")
      nodeGraph(sizeHint, "Foo", "Bar", "Baz").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val limit = math.min(13, sizeHint)
    val logicalQueryBuilder = new LogicalQueryBuilder(this)
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = s"$limit"
      ).build()

    // then
    val runtimeResult = execute(logicalQueryBuilder, runtime)
    runtimeResult should beColumns("labels").withRows(
      Seq.fill(limit)(Array(Array("Foo", "Bar", "Baz"))),
      listInAnyOrder = true
    )
  }

  test("should support multiple labels (on different nodes)") {

    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo", "Bar", "Baz"), "prop")
      nodeGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
      nodeGraph(1, "Bar").foreach(_.setProperty("prop", "the cat sat on the mat"))
      nodeGraph(1, "Baz").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "3"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("labels").withRows(
      Seq(Array(Array("Foo")), Array(Array("Bar")), Array(Array("Baz")))
    )
  }

  test("should support multiple properties (term in several properties of one node)") {

    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "title", "body")
      nodeGraph(1, "Doc").foreach { n =>
        n.setProperty("id", 0)
        n.setProperty("title", "the cat sat on the mat")
        n.setProperty("body", "the cat sat on the mat")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("title", "body"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      ).build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id").withSingleRow(0)

  }

  test("should support multiple properties (term may match in any property across nodes)") {
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "title", "body")
      val nodes = nodeGraph(3, "Doc")
      // node 0: the term is in `title`
      nodes(0).setProperty("id", 0)
      nodes(0).setProperty("title", "the cat sat on the mat")
      nodes(0).setProperty("body", "nothing to see here")
      // node 1: the term is in `body`
      nodes(1).setProperty("id", 1)
      nodes(1).setProperty("title", "an untitled document")
      nodes(1).setProperty("body", "the cat slept all day")
      // node 2: the term is in neither indexed property
      nodes(2).setProperty("id", 2)
      nodes(2).setProperty("title", "the dog ran")
      nodes(2).setProperty("body", "the dog barked")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("title", "body"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      ).build()

    // then: node 0 matches via `title`, node 1 via `body`, node 2 not at all
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id").withRows(singleColumn(Seq(0, 1)))
  }

  test("should fail on too large limits") {

    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo", "Bar", "Baz"), "prop")
      nodeGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
      nodeGraph(1, "Bar").foreach(_.setProperty("prop", "the cat sat on the mat"))
      nodeGraph(1, "Baz").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("labels")
      .projection("labels(n) AS labels")
      .nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "9223372036854775807"
      )
      .build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(
      logicalQuery,
      runtime
    )) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value 9223372036854775807 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'LIMIT' to be of type INTEGER NOT NULL and in the range 0 to 2147483647 but found 9223372036854775807."
    )

  }

  test("should work without issues on the RHS of cartesian product") {
    // given
    val nodes = ArrayBuffer.empty[Node]
    val size = 10
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      nodeGraph(size, "Foo").foreach { n =>
        n.setProperty("prop", "the cat sat on the mat")
        nodes.append(n)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "n")
      .cartesianProduct()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "10000000"
      )
      .input(variables = Seq("i"))
      .build()

    // then
    val input = inputValues((1 to size).map(i => Array[Any](i)): _*)
    val expected = nodes.flatMap(n => (1 to size).map(i => Array(i, n)))
    execute(logicalQuery, runtime, input) should beColumns("i", "n").withRows(expected)
  }

  test("should work without issues on the RHS of apply") {
    // given
    val nodes = ArrayBuffer.empty[Node]
    val size = 10
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      nodeGraph(size, "Foo").foreach { n =>
        n.setProperty("prop", "the cat sat on the mat")
        nodes.append(n)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .build()

    // then
    val input = inputValues((1 to size).map(_ => Array[Any]("cat")): _*)
    val expected = nodes.flatMap(n => (1 to size).map(_ => Array(n)))
    execute(logicalQuery, runtime, input) should beColumns("n").withRows(expected)
  }

  test("should work without issues on the RHS of semiApply") {
    val size = 10
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      nodeGraph(size, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("queryString")
      .semiApply()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .build()

    // then: "cat" matches (RHS non-empty) so it survives; "dog" matches nothing so it is filtered out
    val input = inputValues(Array[Any]("cat"), Array[Any]("dog"), Array[Any]("cat"))
    val expected = Seq(Array[Any]("cat"), Array[Any]("cat"))
    execute(logicalQuery, runtime, input) should beColumns("queryString").withRows(expected)
  }

  test("should work without issues on the RHS of union") {
    val size = 10
    val (fooNodes, barNodes) = givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val foo = nodeGraph(size, "Foo")
      foo.foreach(_.setProperty("prop", "the cat sat on the mat"))
      val bar = nodeGraph(size, "Bar")
      (foo, bar)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .union()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "10000000"
      )
      .nodeByLabelScan("n", "Bar")
      .build()

    // then: union returns every Bar node (LHS) plus every matching Foo node (RHS)
    val expected = (fooNodes ++ barNodes).map(n => Array[Any](n))
    execute(logicalQuery, runtime) should beColumns("n").withRows(expected)
  }

  test("should produce correct row count under LIMIT-above-Apply") {
    // given: every Doc matches "cat", so each input query matches all `matches` docs
    val matches = 5
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(matches, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when: LIMIT sits above the Apply; the prober counts every row that reaches LIMIT
    val numInputRows = 50
    val rowsBelowLimit = new AtomicInteger(0)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .limit(5)
      .prober(countingProbe(rowsBelowLimit))
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .build()

    // then
    val input = inputValues((0 until numInputRows).map(_ => Array[Any]("cat")): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("n").withRows(rowCount(5))

    // LIMIT cancellation propagates upstream across the Apply in every non-Parallel runtime: without it
    // all numInputRows * matches rows reach LIMIT; with it only the first argument's in-flight rows do.
    if (runtimeUsed != Parallel) {
      rowsBelowLimit.get() should be < numInputRows
    }
  }

  test("should not emit more rows than limit, when on RHS of Apply") {
    // given: every Doc matches "cat", so a single argument yields many continuations
    val numValues = 20
    val perValue = 30
    val morselSize = 4
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(perValue, "Doc").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when: LIMIT(1) sits directly on the RHS of the Apply, so each argument is cancelled after one row
    val emissions = new AtomicInteger(0)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.limit(1)
      .|.prober(countingProbe(emissions))
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .withMorselSize(morselSize)
      .build()

    // then
    val input = inputValues((0 until numValues).map(_ => Array[Any]("cat")): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("n").withRows(rowCount(numValues))

    // A correct operator emits at most one morsel per argument before that argument is cancelled.
    // Parallel limit propagation is racy.
    if (runtimeUsed != Parallel) {
      emissions.get() should be <= numValues * morselSize * 2
    }
  }

  test("should not corrupt rows under heavy LIMIT cancellation with multi-match continuation") {
    // given: many Docs all matching "cat", each tagged with a unique id so we can verify the identity of every
    // emitted row; each input query therefore matches a large fraction of the graph
    val graphSize = 200
    givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(graphSize, "Doc").zipWithIndex.foreach {
        case (n, i) =>
          n.setProperty("id", i)
          n.setProperty("prop", "the cat sat on the mat")
      }
    }

    // when: many input rows, each forcing heavy continuation, under a LIMIT above the Apply
    val limit = 5
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("n.id AS id")
      .limit(limit)
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .build()

    // then: the LIMIT must yield exactly `limit` uncorrupted rows — each a real Doc id from the graph. A corrupted
    // reference (an out-of-range id) from morsel reuse during continuation would fail this. Ids may legitimately
    // repeat: every input argument queries "cat" and matches the same Docs, so the LIMIT can take rows from several.
    val input = inputValues((0 until graphSize).map(_ => Array[Any]("cat")): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("id").withRows(matching {
      case rows: Seq[_]
        if rows.size == limit &&
          rows.forall {
            case Array(id: NumberValue) => id.longValue() >= 0 && id.longValue() < graphSize
            case _                      => false
          } =>
    })
  }

  test("should skip cancelled interior arguments on the RHS of a nodeHashJoin") {
    // given: half the Docs carry an lhsKey that matches their argument value, half carry -1
    val nValues = 20
    val nodes = givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(nValues, "Doc").zipWithIndex.map {
        case (n, i) =>
          n.setProperty("prop", "the cat sat on the mat")
          n.setProperty("lhsKey", if (i % 2 == 0) i else -1)
          n
      }
    }

    // when: the hash-join build side is empty for odd arguments, cancelling the search for those
    // interior arguments while even arguments (the FulltextIndexSearch probe side) still produce rows.
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.nodeHashJoin("x")
      .|.|.nodeFulltextIndexSearch(
        node = "x",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "10000000"
      )
      .|.filter("x.lhsKey = value")
      .|.allNodeScan("x", "value")
      .input(variables = Seq("value"))
      .build()

    // then: one row per even-indexed node; odd arguments are cancelled and contribute nothing
    val input = inputValues((0 until nValues).map(v => Array[Any](v)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)
    val expected = nodes.zipWithIndex.collect { case (n, i) if i % 2 == 0 => Array[Any](n) }
    runtimeResult should beColumns("x").withRows(expected, listInAnyOrder = true)
  }

  test("should handle multiple matches per input argument under small morsels") {
    // given: a single query matches many Docs, forcing several continuations at a small morsel size
    val matches = 50
    val nodes = givenGraph {
      nodeIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Doc"), "prop")
      nodeGraph(matches, "Doc").map { n =>
        n.setProperty("prop", "the cat sat on the mat")
        n
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.nodeFulltextIndexSearch(
        node = "n",
        labelNames = Seq("Doc"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .withMorselSize(4)
      .build()

    // then: every matching node is returned for the single argument, across continuations
    val input = inputValues(Array[Any]("cat"))
    val runtimeResult = execute(logicalQuery, runtime, input)
    val expected = nodes.map(n => Array[Any](n))
    runtimeResult should beColumns("n").withRows(expected, listInAnyOrder = true)
  }

}
