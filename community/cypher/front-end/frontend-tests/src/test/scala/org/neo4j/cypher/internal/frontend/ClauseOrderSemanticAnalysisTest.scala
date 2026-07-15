/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.util.Failure
import scala.util.Success

class ClauseOrderSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  def errorWithIsRequiredBetween(
    clause1: String,
    clause2: String,
    offset: Int,
    line: Int,
    column: Int
  ): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(offset, line, column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N24)
        .atPosition(offset, line, column).withParam(GqlParams.StringParam.input1, clause1)
        .withParam(GqlParams.StringParam.input2, clause2)
        .build())
      .build()
    SemanticError(
      gql,
      s"WITH is required between $clause1 and $clause2",
      InputPosition(offset, line, column)
    )
  }

  val updates = Seq(
    ("CREATE", "CREATE (u:A)"),
    ("INSERT", "INSERT (u:A)"),
    ("MERGE", "MERGE (u:A)"),
    ("SET", "MATCH (u) SET u:A"),
    ("SET", "MATCH (u) SET u.p = 5"),
    ("REMOVE", "MATCH (u) REMOVE u:A"),
    ("REMOVE", "MATCH (u) REMOVE u.p"),
    ("DELETE", "MATCH (u) DELETE u"),
    ("DELETE", "MATCH (u) DETACH DELETE u"),
    ("FOREACH", "MATCH (u) FOREACH ( v IN [u] | SET u = {p: 123, q: 'abc'} )"),
    ("FOREACH", "MATCH (u) FOREACH ( i IN [1,2,3] | CREATE (:A {p: i}) )")
  )

  val withs = Seq(
    "WITH u",
    "WITH u, 123 AS foo",
    "WITH *",
    "WITH * WHERE id(u) > 2",
    "WITH * ORDER BY id(u)",
    "WITH * ORDER BY id(u) SKIP 1",
    "WITH * ORDER BY id(u) LIMIT 1"
  )

  val reads = Seq(
    ("MATCH", "MATCH (u)-->()"),
    ("MATCH", "MATCH ()-[:r]->(u)"),
    ("MATCH", "MATCH (v)"),
    ("UNWIND", "UNWIND [1,2,3] AS x"),
    ("UNWIND", "UNWIND u.p AS x"),
    ("CALL", "CALL { UNWIND [1,2,3] AS x RETURN x }"),
    ("LOAD CSV", "LOAD CSV FROM 'https://data.neo4j.com/bands/artists.csv' AS row"),
    ("ORDER BY", "ORDER BY u"),
    ("SKIP", "SKIP 5"),
    ("LIMIT", "LIMIT 5"),
    ("FILTER", "FILTER id(u) > 3"),
    ("LET", "LET x = 123")
  )

  val returnClause = "RETURN *"

  for {
    (_, u) <- updates
    w <- withs
    (clause2, r) <- reads
  } yield {
    test(s"$u $w $r $returnClause") {
      run().assertTryIn {
        case CypherVersion.Cypher5 if clause2 == "FILTER" => {
          case Success(_) => fail(new Exception("FILTER is not part of Cypher 5 syntax"))
          case Failure(_) => ()
        }
        case CypherVersion.Cypher5 if clause2 == "LET" => {
          case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
          case Failure(_) => ()
        }
        case _ => {
          case Success(result) => result.errors shouldBe empty
          case Failure(t)      => fail(t)
        }
      }
    }
  }

  for {
    (clause1, u) <- updates
    (clause2, r) <- reads
  } yield {
    test(s"$u $r $returnClause") {
      run().assertTryIn {
        case CypherVersion.Cypher5 if Seq("ORDER BY", "SKIP", "LIMIT") contains clause2 => {
          case Success(result) => result.errors shouldBe empty
          case Failure(t)      => fail(t)
        }
        case CypherVersion.Cypher5 if clause2 == "FILTER" => {
          case Success(_) => fail(new Exception("FILTER is not part of Cypher 5 syntax"))
          case Failure(_) => ()
        }
        case CypherVersion.Cypher5 if clause2 == "LET" => {
          case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
          case Failure(_) => ()
        }
        case CypherVersion.Cypher5 => {
          case Success(result) => result.errors should contain theSameElementsAs Seq(errorWithIsRequiredBetween(
              clause1,
              clause2,
              u.length + 1,
              1,
              u.length + 2
            ))
          case Failure(t) => fail(t)
        }
        case _ => {
          case Success(result) => result.errors shouldBe empty
          case Failure(t)      => fail(t)
        }
      }
    }
  }
}
