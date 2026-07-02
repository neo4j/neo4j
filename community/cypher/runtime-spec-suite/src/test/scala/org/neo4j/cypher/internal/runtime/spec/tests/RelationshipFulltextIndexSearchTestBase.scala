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
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexSetting
import org.neo4j.graphdb.schema.IndexSettingImpl
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values.longValue

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

abstract class RelationshipFulltextIndexSearchTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](
      edition,
      runtime
    ) {

  private def relationshipGraph(size: Int, typ: String): Seq[Relationship] = {
    val relationships = ArrayBuffer.empty[Relationship]
    val tx = runtimeTestSupport.tx
    (1 to size).foreach { _ =>
      relationships += tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName(typ))
    }
    relationships.toSeq
  }

  test("should project relationship and endpoints from a directed search") {
    // given
    val (a, r, b) = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val a = tx.createNode()
      val b = tx.createNode()
      val r = a.createRelationshipTo(b, RelationshipType.withName("Foo"))
      r.setProperty("prop", "the cat sat on the mat")
      (a, r, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipFulltextIndexSearch(
        "(a)-[r]->(b)",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a", "r", "b").withSingleRow(a, r, b)
  }

  test("should emit both directions from an undirected search") {
    // given
    val (a, r, b) = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val a = tx.createNode()
      val b = tx.createNode()
      val r = a.createRelationshipTo(b, RelationshipType.withName("Foo"))
      r.setProperty("prop", "the cat sat on the mat")
      (a, r, b)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipFulltextIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then: undirected search emits each relationship once per direction
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a", "r", "b").withRows(Seq(Array(a, r, b), Array(b, r, a)))
  }

  test("should only find one row for a self-loop in an undirected search") {
    // given
    val (a, r) = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val a = tx.createNode()
      val r = a.createRelationshipTo(a, RelationshipType.withName("Foo"))
      r.setProperty("prop", "the cat sat on the mat")
      (a, r)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipFulltextIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then: a self-loop has identical endpoints, so it is emitted only once
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a", "r", "b").withSingleRow(a, r, a)
  }

  test("should find matching relationships with score variable") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(r => {
        r.setProperty("id", 1)
        r.setProperty("prop", "the cat sat on the mat")
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id", "score")
      .projection("r.id AS id")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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

  test("should find matching relationships without score variable") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(r => {
        r.setProperty("id", 1)
        r.setProperty("prop", "the cat sat on the mat")
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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

  test("should only find matching relationships") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("prop", if (i % 2 == 0) "the cat sat on the mat" else "the dog barked")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("prop", "cat" + " filler" * (i % 10))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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

  test("should respect the limit (directed)") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "13",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r", "score").withRows(rowCount(math.min(13, sizeHint)))
  }

  test("should respect the limit (undirected)") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "13",
        score = "score"
      )
      .build()

    // then: the limit bounds the index hits, each emitted in both directions
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r", "score").withRows(rowCount(2 * math.min(13, sizeHint)))
  }

  test("should handle limit 0") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "0",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r", "score").withNoRows()
  }

  test("should fail on negative limit") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val skip = sizeHint / 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
    runtimeResult should beColumns("r", "score").withRows(rowCount(sizeHint - skip))
  }

  test("should respect skip combined with limit") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(sizeHint, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "7",
        skip = Some("5"),
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r", "score").withRows(rowCount(math.max(0, math.min(7, sizeHint - 5))))
  }

  test("should skip the highest scoring matches") {
    // each relationship gets a distinct score: more filler words => lower score
    val n = math.min(sizeHint, 50)
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(n, "Foo").zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", "cat" + " filler" * i)
      }
    }

    def query(skip: Option[String]) = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
      beColumns("r", "score").withRows(inOrder(ordered.drop(skip)))
  }

  test("should support analyzer override") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "Hello world"))
    }

    def query(analyzer: Option[String]) = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'Hello'",
        limit = "20",
        analyzer = analyzer
      )
      .build()

    // the default analyzer lowercases terms at index time; a case-sensitive
    // query-time analyzer therefore cannot match the capitalized query token
    execute(query(analyzer = None), runtime) should beColumns("r").withRows(rowCount(1))
    execute(query(analyzer = Some("'whitespace'")), runtime) should beColumns("r").withNoRows()
  }

  test("should fall back to the default analyzer when the analyzer expression is null") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "Hello world"))
    }

    // a null analyzer means "no override": it must behave exactly like analyzer = None and fall back to the index
    // default analyzer (which lowercases and so matches the capitalized query token), unlike the case-sensitive
    // 'whitespace' analyzer which would not match
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'Hello'",
        limit = "20",
        analyzer = Some("NULL")
      )
      .build()

    execute(logicalQuery, runtime) should beColumns("r").withRows(rowCount(1))
  }

  test("should support a per-argument analyzer expression on the RHS of an Apply") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "Hello world"))
    }

    // the analyzer is supplied per input row as an argument: a null argument falls back to the default analyzer
    // (matching the Foo relationship), while the case-sensitive 'whitespace' analyzer does not match
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
    execute(logicalQuery, runtime, input) should beColumns("r").withRows(rowCount(1))
  }

  test("should apply the index analyzer's stemming and honor a query-time analyzer override") {
    givenGraph {
      // the english analyzer stems at index time, so "running" is stored as "run"
      relationshipIndex(Seq("Foo")) { creator =>
        creator
          .withName("FulltextIndex")
          .withIndexType(IndexType.FULLTEXT)
          .withIndexConfiguration(java.util.Map.of[IndexSetting, AnyRef](IndexSettingImpl.FULLTEXT_ANALYZER, "english"))
          .on("prop")
      }
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cats are running"))
    }

    def query(analyzer: Option[String]) = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'running'",
        limit = "20",
        analyzer = analyzer
      )
      .build()

    // no override -> the query uses the index's english analyzer, which stems
    // "running" -> "run" and matches the stored stem
    execute(query(analyzer = None), runtime) should beColumns("r").withRows(rowCount(1))
    // the whitespace analyzer does not stem, so the query term "running" misses the stored stem "run"
    execute(query(analyzer = Some("'whitespace'")), runtime) should beColumns("r").withNoRows()
  }

  test("should be able to query the index with multiple inputs from a property") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      Seq("cat", "dog", "cat dog").zip(relationshipGraph(3, "Foo")).foreach {
        case (text, r) => r.setProperty("prop", text)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r2", "score")
      .apply()
      .|.relationshipFulltextIndexSearch(
        "()-[r2]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "r1.prop",
        limit = "20",
        score = "score",
        argumentIds = Set("r1")
      )
      .relationshipTypeScan("()-[r1:Foo]->()")
      .build()

    // then: 'cat' matches 2 rels, 'dog' matches 2 rels, 'cat dog' matches all 3
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r2", "score").withRows(rowCount(7))
  }

  test("should return empty if query string is null") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "NULL",
        limit = "20",
        score = "score"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r", "score").withNoRows()
  }

  test("should return empty for a null query string even with a negative limit") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // a null queryString must short-circuit to empty before the limit is validated, so a negative limit must not
    // throw — consistent across interpreted, slotted, pipelined and parallel
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "NULL",
        limit = "-1"
      )
      .build()

    execute(logicalQuery, runtime) should beColumns("r").withNoRows()
  }

  test("should fail if query string has the wrong type") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
      relationshipIndex("FulltextIndex", IndexType.RANGE, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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

  test("should support multiple types (directed)") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo", "Bar", "Baz"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
      relationshipGraph(1, "Bar").foreach(_.setProperty("prop", "the cat sat on the mat"))
      relationshipGraph(1, "Baz").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("type")
      .projection("type(r) AS type")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("type").withRows(singleColumn(Seq("Foo", "Bar", "Baz")))
  }

  test("should support multiple properties (term in several properties of one relationship)") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "title", "body")
      relationshipGraph(1, "Foo").foreach { r =>
        r.setProperty("id", 0)
        r.setProperty("title", "the cat sat on the mat")
        r.setProperty("body", "the cat sat on the mat")
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("title", "body"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id").withSingleRow(0)
  }

  test("should support multiple properties (term may match in any property across relationships)") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "title", "body")
      val rels = relationshipGraph(3, "Foo")
      // relationship 0: the term is in `title`
      rels(0).setProperty("id", 0)
      rels(0).setProperty("title", "the cat sat on the mat")
      rels(0).setProperty("body", "nothing to see here")
      // relationship 1: the term is in `body`
      rels(1).setProperty("id", 1)
      rels(1).setProperty("title", "an untitled document")
      rels(1).setProperty("body", "the cat slept all day")
      // relationship 2: the term is in neither indexed property
      rels(2).setProperty("id", 2)
      rels(2).setProperty("title", "the dog ran")
      rels(2).setProperty("body", "the dog barked")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("title", "body"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then: relationship 0 matches via `title`, relationship 1 via `body`, relationship 2 not at all
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("id").withRows(singleColumn(Seq(0, 1)))
  }

  test("should support multiple types (undirected)") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo", "Bar", "Baz"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
      relationshipGraph(1, "Bar").foreach(_.setProperty("prop", "the cat sat on the mat"))
      relationshipGraph(1, "Baz").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("type")
      .projection("type(r) AS type")
      .relationshipFulltextIndexSearch(
        "()-[r]-()",
        typeNames = Seq("Foo", "Bar", "Baz"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "20"
      )
      .build()

    // then: each match is emitted once per direction
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("type").withRows(singleColumn(Seq("Foo", "Foo", "Bar", "Bar", "Baz", "Baz")))
  }

  test("should fail on too large limits") {
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(1, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "score")
      .relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "9223372036854775807",
        score = "score"
      )
      .build()

    // then
    the[InvalidArgumentException] thrownBy consume(execute(logicalQuery, runtime)) shouldBe gqlStatus(
      GqlStatusInfoCodes.STATUS_22003,
      "error: data exception - numeric value out of range. The numeric value 9223372036854775807 is outside the required range."
    ).withCause(
      GqlStatusInfoCodes.STATUS_22N03,
      "error: data exception - specified numeric value out of range. Expected 'LIMIT' to be of type INTEGER NOT NULL and in the range 0 to 2147483647 but found 9223372036854775807."
    )
  }

  test("should work without issues on the RHS of cartesian product") {
    // given
    val size = 10
    val rels = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val rels = relationshipGraph(size, "Foo")
      rels.foreach(_.setProperty("prop", "the cat sat on the mat"))
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "r")
      .cartesianProduct()
      .|.relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "10000000"
      )
      .input(variables = Seq("i"))
      .build()

    // then
    val input = inputValues((1 to size).map(i => Array[Any](i)): _*)
    val expected = rels.flatMap(r => (1 to size).map(i => Array(i, r)))
    execute(logicalQuery, runtime, input) should beColumns("i", "r").withRows(expected)
  }

  test("should work without issues on the RHS of apply") {
    // given
    val size = 10
    val rels = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val rels = relationshipGraph(size, "Foo")
      rels.foreach(_.setProperty("prop", "the cat sat on the mat"))
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
    val expected = rels.flatMap(r => (1 to size).map(_ => Array(r)))
    execute(logicalQuery, runtime, input) should beColumns("r").withRows(expected)
  }

  test("should work without issues on the RHS of semiApply") {
    val size = 10
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(size, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("queryString")
      .semiApply()
      .|.relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
    val (fooRels, barRels) = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      val foo = relationshipGraph(size, "Foo")
      foo.foreach(_.setProperty("prop", "the cat sat on the mat"))
      val bar = relationshipGraph(size, "Bar")
      (foo, bar)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "10000000"
      )
      .relationshipTypeScan("()-[r:Bar]->()")
      .build()

    // then: union returns every Bar relationship (LHS) plus every matching Foo relationship (RHS)
    val expected = (fooRels ++ barRels).map(r => Array[Any](r))
    execute(logicalQuery, runtime) should beColumns("r").withRows(expected)
  }

  test("should produce correct row count under LIMIT-above-Apply") {
    // given: every Foo relationship matches "cat", so each input query matches all `matches` rels
    val matches = 5
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(matches, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when: LIMIT sits above the Apply; the prober counts every row that reaches LIMIT
    val numInputRows = 50
    val rowsBelowLimit = new AtomicInteger(0)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .limit(5)
      .prober(countingProbe(rowsBelowLimit))
      .apply()
      .|.relationshipFulltextIndexSearch(
        "()-[r]->()",
        typeNames = Seq("Foo"),
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
    runtimeResult should beColumns("r").withRows(rowCount(5))

    // LIMIT cancellation propagates upstream across the Apply in every non-Parallel runtime: without it
    // all numInputRows * matches rows reach LIMIT; with it only the first argument's in-flight rows do.
    if (runtimeUsed != Parallel) {
      rowsBelowLimit.get() should be < numInputRows
    }
  }

  test("should not emit more rows than limit, when on RHS of Apply (undirected)") {
    // given: every Foo relationship matches "cat"; an undirected search re-emits each match in both
    // directions, so a single argument yields many continuations
    val numValues = 20
    val perValue = 30
    val morselSize = 4
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(perValue, "Foo").foreach(_.setProperty("prop", "the cat sat on the mat"))
    }

    // when: LIMIT(1) sits directly on the RHS of the Apply, so each argument is cancelled after one row
    val emissions = new AtomicInteger(0)
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.limit(1)
      .|.prober(countingProbe(emissions))
      .|.relationshipFulltextIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
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
    runtimeResult should beColumns("r").withRows(rowCount(numValues))

    // A correct operator emits at most one morsel per argument before that argument is cancelled,
    // even though the undirected re-emit doubles the per-match rows. Parallel limit propagation is racy.
    if (runtimeUsed != Parallel) {
      emissions.get() should be <= numValues * morselSize * 2
    }
  }

  test("should not corrupt rows under heavy LIMIT cancellation with multi-match continuation (undirected)") {
    // given: many Foo relationships all matching "cat", each tagged with a unique id so we can verify the identity
    // of every emitted row; each input query therefore matches a large fraction of them
    val graphSize = 200
    givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      relationshipGraph(graphSize, "Foo").zipWithIndex.foreach {
        case (r, i) =>
          r.setProperty("id", i)
          r.setProperty("prop", "the cat sat on the mat")
      }
    }

    // when: many input rows, each forcing heavy (doubled) continuation, under a LIMIT above the Apply
    val limit = 5
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("id")
      .projection("r.id AS id")
      .limit(limit)
      .apply()
      .|.relationshipFulltextIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "queryString",
        limit = "10000000",
        argumentIds = Set("queryString")
      )
      .input(variables = Seq("queryString"))
      .build()

    // then: the LIMIT must yield exactly `limit` uncorrupted rows — each a real Foo id from the graph. A corrupted
    // reference (out-of-range id) from morsel reuse during the doubled undirected continuation would fail this.
    // Ids may legitimately repeat: an undirected match emits the same relationship in both directions.
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

  test("should emit both directions across continuations under small morsels (undirected)") {
    // given: many matching relationships between distinct node pairs (no self-loops)
    val size = 50
    val triples = givenGraph {
      relationshipIndex("FulltextIndex", IndexType.FULLTEXT, Seq("Foo"), "prop")
      (0 until size).map { _ =>
        val a = tx.createNode()
        val b = tx.createNode()
        val r = a.createRelationshipTo(b, RelationshipType.withName("Foo"))
        r.setProperty("prop", "the cat sat on the mat")
        (a, r, b)
      }
    }

    // when: a single argument matches every relationship; a small morsel forces continuations, so the
    // bidirectional re-emit (forward then reverse) must survive morsel boundaries
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r", "b")
      .relationshipFulltextIndexSearch(
        "(a)-[r]-(b)",
        typeNames = Seq("Foo"),
        properties = Seq("prop"),
        indexName = "FulltextIndex",
        queryString = "'cat'",
        limit = "10000000"
      )
      .withMorselSize(4)
      .build()

    // then: each relationship is emitted once per direction
    val runtimeResult = execute(logicalQuery, runtime)
    val expected = triples.flatMap { case (a, r, b) => Seq(Array[Any](a, r, b), Array[Any](b, r, a)) }
    runtimeResult should beColumns("a", "r", "b").withRows(expected, listInAnyOrder = true)
  }
}
