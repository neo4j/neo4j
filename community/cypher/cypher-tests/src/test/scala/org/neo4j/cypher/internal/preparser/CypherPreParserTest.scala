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
package org.neo4j.cypher.internal.preparser

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb.config.Setting
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.language.implicitConversions

class CypherPreParserTest extends CommunityCypherTestSuite with TableDrivenPropertyChecks {

  private val queries = Table[String, (List[PreParserOption], InputPosition)](
    ("query", "expected"),
    (
      "RETURN 1 / 0.5 as number",
      (List.empty, (1, 1, 0))
    ),
    (
      "RETURN .1e9 AS literal",
      (List.empty, (1, 1, 0))
    ),
    (
      "RETURN '\\u0000H'",
      (List.empty, (1, 1, 0))
    ),
    (
      "PROFILE MATCH",
      (List(PreParserOption.profile((1, 1, 0))), (1, 9, 8))
    ),
    (
      "EXPLAIN MATCH",
      (List(PreParserOption.explain((1, 1, 0))), (1, 9, 8))
    ),
    (
      "EXPLAIN SCOPE MATCH",
      (List(PreParserOption.explain((1, 1, 0)), PreParserOption.scope((1, 9, 8))), (1, 15, 14))
    ),
    (
      "EXPLAIN PLAN MATCH",
      (List(PreParserOption.explain((1, 1, 0)), PreParserOption.plan((1, 9, 8))), (1, 14, 13))
    ),
    (
      "CYPHER WITH YALL",
      (List.empty, (1, 8, 7))
    ),
    (
      "CYPHER planner=cost RETURN",
      (List(PreParserOption.generic("planner", "cost", (1, 8, 7))), (1, 21, 20))
    ),
    (
      "CYPHER planner = idp RETURN",
      (List(PreParserOption.generic("planner", "idp", (1, 8, 7))), (1, 22, 21))
    ),
    (
      "CYPHER planner =dp RETURN",
      (List(PreParserOption.generic("planner", "dp", (1, 8, 7))), (1, 20, 19))
    ),
    (
      "CYPHER runtime=interpreted RETURN",
      (List(PreParserOption.generic("runtime", "interpreted", (1, 8, 7))), (1, 28, 27))
    ),
    (
      "CYPHER runtime=parallel parallelRuntimeConfig=leverageOrder RETURN",
      (
        List(
          PreParserOption.generic("runtime", "parallel", (1, 8, 7)),
          PreParserOption.generic("parallelRuntimeConfig", "leverageOrder", (1, 25, 24))
        ),
        (1, 61, 60)
      )
    ),
    (
      "CYPHER planner=cost runtime=interpreted RETURN",
      (
        List(
          PreParserOption.generic("planner", "cost", (1, 8, 7)),
          PreParserOption.generic("runtime", "interpreted", (1, 21, 20))
        ),
        (1, 41, 40)
      )
    ),
    (
      "CYPHER planner=dp runtime=interpreted RETURN",
      (
        List(
          PreParserOption.generic("planner", "dp", (1, 8, 7)),
          PreParserOption.generic("runtime", "interpreted", (1, 19, 18))
        ),
        (1, 39, 38)
      )
    ),
    (
      "CYPHER planner=idp runtime=interpreted RETURN",
      (
        List(
          PreParserOption.generic("planner", "idp", (1, 8, 7)),
          PreParserOption.generic("runtime", "interpreted", (1, 20, 19))
        ),
        (1, 40, 39)
      )
    ),
    (
      "CYPHER updateStrategy=eager RETURN",
      (List(PreParserOption.generic("updateStrategy", "eager", (1, 8, 7))), (1, 29, 28))
    ),
    (
      "CYPHER runtime=slotted RETURN",
      (List(PreParserOption.generic("runtime", "slotted", (1, 8, 7))), (1, 24, 23))
    ),
    (
      "CYPHER expressionEngine=interpreted RETURN",
      (List(PreParserOption.generic("expressionEngine", "interpreted", (1, 8, 7))), (1, 37, 36))
    ),
    (
      "CYPHER expressionEngine=compiled RETURN",
      (List(PreParserOption.generic("expressionEngine", "compiled", (1, 8, 7))), (1, 34, 33))
    ),
    (
      "CYPHER replan=force RETURN",
      (List(PreParserOption.generic("replan", "force", (1, 8, 7))), (1, 21, 20))
    ),
    (
      "CYPHER replan=skip RETURN",
      (List(PreParserOption.generic("replan", "skip", (1, 8, 7))), (1, 20, 19))
    ),
    (
      "CYPHER planner=cost MATCH(n:Node) WHERE n.prop = 3 RETURN n",
      (List(PreParserOption.generic("planner", "cost", (1, 8, 7))), (1, 21, 20))
    ),
    (
      "CREATE ({name: 'USING PERIODIC COMMIT'})",
      (List.empty, (1, 1, 0))
    ),
    (
      "match (c:CYPHER) WITH c as debug, 'profile' as explain, RETURN debug, explain",
      (List.empty, (1, 1, 0))
    ),
    (
      "match (runtime:C) WITH 'string' as slotted WHERE runtime=slotted RETURN runtime",
      (List.empty, (1, 1, 0))
    ),
    (
      "EXPLAIN LOAD CSV WITH HEADERS FROM 'file:///fake.csv' AS row CREATE ()-[:T]->()",
      (List(PreParserOption.explain((1, 1, 0))), (1, 9, 8))
    ),
    (
      "//TESTING \n //TESTING \n EXPLAIN MATCH (n) //TESTING\n MATCH (b:X) return n,b Limit 1",
      (List(PreParserOption.explain((3, 2, 24))), (3, 10, 32))
    ),
    (
      " EXPLAIN MATCH (n) RETURN",
      (List(PreParserOption.explain((1, 2, 1))), (1, 10, 9))
    ),
    (
      " /* Some \n comment */ EXPLAIN MATCH (n) RETURN /* Some \n comment */ n",
      (List(PreParserOption.explain((2, 13, 22))), (2, 21, 30))
    ),
    (
      "EXPLAIN /* Some \n comment */ MATCH (n) RETURN /* Some \n comment */ n",
      (List(PreParserOption.explain((1, 1, 0))), (2, 13, 29))
    ),
    (
      "//TESTING \n //TESTING \n EXPLAIN MATCH (n)\n MATCH (b:X) return n,b Limit 1",
      (List(PreParserOption.explain((3, 2, 24))), (3, 10, 32))
    ),
    (
      " EXPLAIN/* 2 */ // \n  query",
      (List(PreParserOption.explain((1, 2, 1))), (2, 3, 22))
    ),
    (
      "CYPHER // \n planner // \n = idp query",
      (List(PreParserOption.generic("planner", "idp", (2, 2, 12))), (3, 8, 31))
    ),
    (
      " /* 1 */ EXPLAIN CYPHER\n planner /* 2 */ // \n =  /** 3 */ // \n cost MATCH /* 4 */ s ",
      (List(PreParserOption.explain((1, 10, 9)), PreParserOption.generic("planner", "cost", (2, 2, 25))), (4, 7, 68))
    )
  )

  test("run the tests") {
    forAll(queries) {
      case (query, (options, pos)) =>
        for {
          dbDefaultVersion <- CypherVersion.values()
          explicitVersion <- CypherVersion.values().map(Some.apply) :+ Option.empty[CypherVersion]
        } {
          val extraOptions = explicitVersion.map(v => PreParserOption.version(v.versionName, InputPosition(0, 1, 1)))
          val extraPrefix = explicitVersion.map(_.description + "\n").getOrElse("")
          val addedOffset = extraPrefix.length
          val addedLines = extraPrefix.count(_ == '\n')
          val queryWithExtra = extraPrefix + query
          val newPos =
            if (addedOffset == 0) pos else InputPosition(pos.offset + addedOffset, pos.line + addedLines, pos.column)
          withClue(s"Failed on query: $query\nTransformed query: $queryWithExtra\n") {
            parse(queryWithExtra, dbDefaultVersion) shouldBe PreParsedQuery(
              queryWithExtra.substring(newPos.offset),
              queryWithExtra,
              PreParser.queryOptions(
                PreParsedStatement(queryWithExtra, options ++ extraOptions, newPos),
                CypherConfiguration.fromConfig(Config.defaults(
                  // Might need to be enabled when the next experimental version appear:
                  // GraphDatabaseInternalSettings.enable_experimental_cypher_versions, java.lang.Boolean.TRUE
                  GraphDatabaseInternalSettings.cypher_enable_scope_queries,
                  java.lang.Boolean.TRUE
                )),
                dbDefaultVersion
              ),
              Seq.empty
            )
          }
        }
    }
  }

  private def preParserWith(settings: (Setting[_], AnyRef)*) = new CachingPreParser(
    CypherConfiguration.fromConfig(Config.defaults(settings.toMap.asJava)),
    new LFUCache[PreParsedQuery.CacheKey, PreParsedQuery](TestExecutorCaffeineCacheFactory, 0)
  )

  private def parse(queryText: String, version: CypherVersion): PreParsedQuery = {
    preParserWith(
      // Might need to be enabled when the next experimental version appear: GraphDatabaseInternalSettings.enable_experimental_cypher_versions -> java.lang.Boolean.TRUE
      GraphDatabaseInternalSettings.cypher_enable_scope_queries -> java.lang.Boolean.TRUE
    ).preParse(queryText, version)
  }

  implicit private def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
