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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.database.NormalizedDatabaseName

class RemoveDuplicateUseClausesTest extends CypherFunSuite with RewritePhaseTest with AstConstructionTestSupport {

  val sessionDatabaseName: String = new NormalizedDatabaseName("sessionDb").name()
  override def sessionDatabase: String = sessionDatabaseName
  override def targetsComposite: Boolean = true

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    RemoveDuplicateUseClauses

  override def semanticFeatures: Seq[SemanticFeature] = Seq(SemanticFeature.UseAsMultipleGraphsSelector)

  test("remove leading session database use clause (case insensitive)") {
    assertRewritten(
      "USE SessionDB MATCH (n) RETURN *",
      "MATCH(n) RETURN *"
    )
  }

  test("do not remove nested session database use clause (case insensitive)") {
    assertNotRewritten(
      """USE `neo4j`
        |CALL {
        |   USE `sessiondb`
        |   MATCH (n)
        |   RETURN n
        |}
        |RETURN n""".stripMargin
    )
  }

  test("remove session database in union") {
    assertRewritten(
      """USE `neo4j`
        |MATCH (n)
        |RETURN n
        |UNION
        |USE `sessiondb`
        |MATCH (n)
        |RETURN n""".stripMargin,
      """USE `neo4j`
        |MATCH (n)
        |RETURN n
        |UNION
        |MATCH (n)
        |RETURN n""".stripMargin
    )
  }

  test("remove nested session database in subquery") {
    assertRewritten(
      """USE `neo4j`
        |CALL {
        | USE `neo4j`
        | RETURN 1 as n
        |}
        |RETURN n""".stripMargin,
      """USE `neo4j`
        |CALL {
        | RETURN 1 as n
        |}
        |RETURN n""".stripMargin
    )
  }

  test("remove use clause in union in subquery") {
    assertRewritten(
      """CALL {
        | USE `neo4j`
        | RETURN 1 as n
        | UNION
        | USE `sessionDb`
        | RETURN 1 as n
        |}
        |RETURN n""".stripMargin,
      """CALL {
        | USE `neo4j`
        | RETURN 1 as n
        | UNION
        | RETURN 1 as n
        |}
        |RETURN n""".stripMargin.stripMargin
    )
  }

  test("remove graph reference") {
    assertRewritten(
      """USE graph.byName("neo4j")
        |CALL {
        |  USE graph.byName("neo4j")
        |  RETURN 1 as n
        |}
        |RETURN n
        |""".stripMargin,
      """USE graph.byName("neo4j")
        |CALL {
        |  RETURN 1 as n
        |}
        |RETURN n
        |""".stripMargin
    )
  }

  test("do not remove different case graph references in graph.byName") {
    assertNotRewritten(
      """USE graph.byName("Neo4j")
        |CALL {
        |  USE graph.byName("neo4j")
        |  RETURN 1 as n
        |}
        |RETURN 1
        |""".stripMargin
    )
  }

  test("remove direct graph reference case sensitive") {
    // This is not supported in the old stack
    assertRewritten(
      """USE neo4j
        |CALL {
        |  USE NEO4J
        |  RETURN 1 as n
        |}
        |RETURN 1
        |""".stripMargin,
      """USE neo4j
        |CALL {
        |  RETURN 1 as n
        |}
        |RETURN 1
        |""".stripMargin
    )
  }

  test("remove graph reference in union all") {
    // This is not supported in the old stack
    assertRewritten(
      """USE neo4j
        |RETURN 1 as n
        |UNION ALL
        |USE sessionDb
        |RETURN 1 as n
        |""".stripMargin,
      """USE neo4j
        |RETURN 1 as n
        |UNION ALL
        |RETURN 1 as n
        |""".stripMargin
    )
  }

  test("remove nested graph reference") {
    // This is not supported in the old stack
    assertRewritten(
      """USE neo4j
        |CALL {
        |  CALL {
        |    USE neo4j
        |    RETURN 1 as n
        |  }
        |  RETURN n
        |}
        |RETURN 1 as n
        |UNION ALL
        |USE sessionDb
        |RETURN 1 as n
        |""".stripMargin,
      """USE neo4j
        |CALL {
        |  CALL {
        |    RETURN 1 as n
        |  }
        |  RETURN n
        |}
        |RETURN 1 as n
        |UNION ALL
        |RETURN 1 as n
        |""".stripMargin
    )
  }

  test("do not remove graphs references in triple union") {
    // This is not supported in the old stack
    assertNotRewritten(
      """
        |USE neo4j
        |RETURN 1 as n
        |UNION
        |USE neo4j
        |RETURN 1 as n
        |UNION
        |USE neo4j
        |RETURN 1 as n
        |""".stripMargin
    )
  }

  test("remove session database graph references in triple union") {
    // This is not supported in the old stack
    assertRewritten(
      """
        |USE sessionDb
        |RETURN 1 as n
        |UNION
        |USE sessionDb
        |RETURN 1 as n
        |UNION
        |USE sessionDb
        |RETURN 1 as n
        |""".stripMargin,
      """
        |RETURN 1 as n
        |UNION
        |RETURN 1 as n
        |UNION
        |RETURN 1 as n
        |""".stripMargin
    )
  }

  test("only keep graph references that are not the session database in triple union") {
    // This is not supported in the old stack
    assertRewritten(
      """
        |USE neo4j
        |RETURN 1 as n
        |UNION
        |USE sessionDb
        |RETURN 1 as n
        |UNION
        |USE sessionDb
        |RETURN 1 as n
        |""".stripMargin,
      """
        |USE neo4j
        |RETURN 1 as n
        |UNION
        |RETURN 1 as n
        |UNION
        |RETURN 1 as n
        |""".stripMargin
    )
  }
}
