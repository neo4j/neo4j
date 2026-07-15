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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.gqlstatus.GqlHelper

import scala.jdk.CollectionConverters.SeqHasAsJava

class PathModeSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  private def errMatchModePathModeUnsupported(pathMode: String, pos: InputPosition): SemanticError =
    SemanticError(
      GqlHelper.getGql42001_42N60(pathMode, pos.offset, pos.line, pos.column),
      s"REPEATABLE ELEMENTS with $pathMode path mode is not supported.",
      pos
    )

  private def errPathModeMixUnsupported(pathModes: Set[String], pos: InputPosition): SemanticError =
    SemanticError(
      GqlHelper.getGql42001_42N61(pathModes.toList.asJava, pos.offset, pos.line, pos.column),
      s"Mixing path modes ${pathModes.mkString(", ")} in the same graph pattern is not supported.",
      pos
    )

  private def errLegacyShortestWithPathMode(fun: String, pos: InputPosition): SemanticError =
    SemanticError(
      GqlHelper.getGql42001_42I39(fun, pos.offset, pos.line, pos.column),
      "Mixing shortestPath/allShortestPaths with path selectors (e.g. `ANY SHORTEST`), " +
        "explicit match modes (e.g. `DIFFERENT RELATIONSHIPS`) or explicit path modes (e.g. `ACYCLIC`) is not allowed.",
      pos
    )

  // This restriction is a temporary and will be lifted by implementing PLAN-3015
  private def errVarLengthWithPathMode(varLength: String, pathMode: String, pos: InputPosition): SemanticError =
    SemanticError(
      GqlHelper.getGql42001_51N26(
        "Using explicit path modes on a pattern containing a variable-length relationship",
        s"`$pathMode` on variable-length relationships",
        pos.offset,
        pos.line,
        pos.column
      ),
      s"Using a variable-length relationship such as `$varLength` together with explicit path mode `$pathMode` is not available.",
      pos
    )

  test("MATCH WALK (a)-->(b) RETURN *") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH TRAIL (a)-->(b) RETURN *") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH ACYCLIC (a)-->(b) RETURN *") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH REPEATABLE ELEMENTS (a)-->(b) RETURN *") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH REPEATABLE ELEMENTS WALK (a)-->(b) RETURN *") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH REPEATABLE ELEMENTS TRAIL (a)-->(b) RETURN *") {
    runWith(
      defaultQuery,
      disabledCypherVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(errMatchModePathModeUnsupported("TRAIL", p(26, 1, 27)))
  }

  test("MATCH REPEATABLE ELEMENTS ACYCLIC (a)-->(b) RETURN *") {
    runWith(
      defaultQuery,
      disabledCypherVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(errMatchModePathModeUnsupported("ACYCLIC", p(26, 1, 27)))
  }

  test("MATCH p = ACYCLIC (s {i: 1})-->+(s), q = ACYCLIC (s)-->+({i: 3}) RETURN p, q") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH p = ACYCLIC (s {i: 1})-->+(s), q = TRAIL (s)-->+({i: 3}) RETURN p, q") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5))
      .hasErrors(errPathModeMixUnsupported(Set("ACYCLIC", "TRAIL"), p(6, 1, 7)))
  }

  test("MATCH p = ACYCLIC (s {i: 1})-->+(s), q = (s)-->+({i: 3}) RETURN p, q") {
    runWith(
      defaultQuery,
      disabledCypherVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(errPathModeMixUnsupported(Set("ACYCLIC", "WALK"), p(6, 1, 7)))
  }

  test("MATCH REPEATABLE ELEMENTS p = WALK (s {i: 1})-->{,100}(s), q = (s)-->{,1}({i: 3}) RETURN p, q") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  val explicitPathModes: Seq[String] = Seq(
    "WALK",
    "TRAIL",
    "ACYCLIC"
  )

  test("should fail on legacy shortestPath with explicit path modes") {
    explicitPathModes.foreach { pathMode =>
      val modeLength = pathMode.length
      val query =
        s"""MATCH p = $pathMode allShortestPaths((s {i: 1})-[*1..100]->(t))
           |RETURN p""".stripMargin
      withClue(query) {
        runWith(query, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
          errVarLengthWithPathMode("-[*1..100]->", pathMode, p(38 + modeLength, 1, 39 + modeLength)),
          errLegacyShortestWithPathMode("allShortestPaths", p(11 + modeLength, 1, 12 + modeLength))
        )
      }
    }
  }

  test("MATCH p = allShortestPaths((s {i: 1})-[*1..100]->(t)) RETURN p") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("MATCH p = ACYCLIC (s {i: 1})-->+(s) MATCH q = allShortestPaths((s {i: 1})-[*1..100]->(t)) RETURN p, q") {
    runWith(defaultQuery, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  // Temporary restriction
  test("doesn't allow mixing var-length with explicit path modes") {
    runWith(
      "MATCH ACYCLIC (n)-[*]->(m) RETURN *",
      disabledCypherVersions = Set(CypherVersion.Cypher5)
    ).hasErrors(errVarLengthWithPathMode(
      "-[*]->",
      "ACYCLIC",
      p(17, 1, 18)
    ))
  }
}
