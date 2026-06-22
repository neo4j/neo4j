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

}
