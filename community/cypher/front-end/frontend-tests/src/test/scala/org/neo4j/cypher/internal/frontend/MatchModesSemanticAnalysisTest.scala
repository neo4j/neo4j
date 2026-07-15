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
import org.neo4j.cypher.internal.frontend.helpers.ShortestSyntax
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.gqlstatus.GqlHelper

class MatchModesSemanticAnalysisTest extends CypherFunSuite
    with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName
    with ShortestSyntax {

  override def defaultQuery: String = s"MATCH $testName RETURN *"

  def unboundRepeatableElementsSemanticError(pos: InputPosition): SemanticError = SemanticError(
    GqlHelper.getGql42001_42N53(pos.offset, pos.line, pos.column),
    "The quantified path pattern may yield an infinite number of rows under match mode 'REPEATABLE ELEMENTS'. " +
      "Add an upper bound to the quantified path pattern.",
    pos
  )

  def differentRelationshipsSelectivePathPatternSemanticError(
    pos: InputPosition,
    explicitMatchModesSupported: Boolean
  ): SemanticError = {
    val matchModeTip = if (explicitMatchModesSupported) {
      " You may want to use multiple MATCH clauses, or you might want to consider using the REPEATABLE ELEMENTS match mode."
    } else {
      ""
    }

    SemanticError(
      GqlHelper.getGql42001_42I45(matchModeTip, pos.offset, pos.line, pos.column),
      "Multiple path patterns cannot be used in the same clause in combination with a selective path selector." + matchModeTip,
      pos
    )
  }

  private val legacyShortestWithGpmFeaturesErrorMsg =
    "Mixing shortestPath/allShortestPaths with path selectors (e.g. `ANY SHORTEST`), explicit match modes " +
      "(e.g. `DIFFERENT RELATIONSHIPS`) or explicit path modes (e.g. `ACYCLIC`) is not allowed."

  test("REPEATABLE ELEMENTS (c5)") {
    // explicit match mode is not supported in Cypher 5
    run(disabledCypherVersions = Set(CypherVersion.Cypher25)).hasError(
      GqlHelper.getGql42001_42N54("REPEATABLE ELEMENTS", 6, 1, 7),
      "Match modes such as `REPEATABLE ELEMENTS` are not supported in Cypher 5.",
      p(6, 1, 7)
    )
  }

  test("DIFFERENT RELATIONSHIPS (c5)") {
    // explicit match mode is not supported in Cypher 5
    run(disabledCypherVersions = Set(CypherVersion.Cypher25)).hasError(
      GqlHelper.getGql42001_42N54("DIFFERENT RELATIONSHIPS", 6, 1, 7),
      "Match modes such as `DIFFERENT RELATIONSHIPS` are not supported in Cypher 5.",
      p(6, 1, 7)
    )
  }

  test("(a)") {
    // running with implicit "DIFFERENT RELATIONSHIPS" match mode should not fail
    run().hasNoErrors
  }

  test("DIFFERENT RELATIONSHIPS ((a)-[:REL]->(b)){2}") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("((a)-[:REL]->(b)){2}, ((c)-[:REL]->(d))+") {
    run().hasNoErrors
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b)){2}") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b)){1,}") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(26, 1, 27))
    )
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b))+") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(26, 1, 27))
    )
  }

  test("REPEATABLE ELEMENTS (a)-[:REL*]->(b)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(26, 1, 27))
    )
  }

  test("REPEATABLE ELEMENTS SHORTEST 2 PATH ((a)-[:REL]->(b))+") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(42, 1, 43))
    )
  }

  test("REPEATABLE ELEMENTS ANY ((a)-[:REL]->(b))+") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(30, 1, 31))
    )
  }

  test("REPEATABLE ELEMENTS SHORTEST 1 PATH GROUPS ((a)-[:REL]->(b))+") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(49, 1, 50))
    )
  }

  test("shortestPath((a)-[:REL*]->(b)), shortestPath((c)-[:REL*]->(d))") {
    run().hasNoErrors
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b)){2}, ((c)-[:REL]->(d))+") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(48, 1, 49))
    )
  }

  test("REPEATABLE ELEMENTS ((a)-[:REL]->(b))+, ((c)-[:REL]->(d))+") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(26, 1, 27)),
      unboundRepeatableElementsSemanticError(p(46, 1, 47))
    )
  }

  test("REPEATABLE ELEMENTS SHORTEST 1 PATH (a)-[:REL*]->(b), SHORTEST 1 PATH (c)-[:REL*]->(d)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      unboundRepeatableElementsSemanticError(p(42, 1, 43)),
      unboundRepeatableElementsSemanticError(p(76, 1, 77))
    )
  }

  test("DIFFERENT RELATIONSHIPS SHORTEST 1 PATH (a)-[:REL*]->(b), SHORTEST 1 PATH (c)-[:REL*]->(d)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      differentRelationshipsSelectivePathPatternSemanticError(p(46, 1, 47), true)
    )
  }

  test("SHORTEST 1 PATH (a)-[:REL*]->(b), (c)-[:REL]->(d)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      differentRelationshipsSelectivePathPatternSemanticError(p(22, 1, 23), true)
    )
    run(disabledCypherVersions = Set(CypherVersion.Cypher25)).hasErrors(
      differentRelationshipsSelectivePathPatternSemanticError(p(22, 1, 23), false)
    )
  }

  private def invalidEquijoinErrorMsg(varName: String) = s"Variable `$varName` already declared"

  // Under REPEATABLE ELEMENTS, we allow mixing selective path patterns alongside other path patterns
  allSelectiveSelectors.foreach { selector =>
    allSelectors.foreach { otherSelector =>
      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p1 = $selector (a)-->{1, 32}(b)-->(c),
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }
      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(b)-->(c)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }

      // Newly introduced strict interior nodes of selective path patterns may not be reused in the same MATCH clause
      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p1 = $selector (a)-->{1, 32}(x)-->(c),
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(
          testName,
          disabledCypherVersions = Set(CypherVersion.Cypher5)
        ).hasErrorMessages(invalidEquijoinErrorMsg("x"))
      }
      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(x)-->(c)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(
          testName,
          disabledCypherVersions = Set(CypherVersion.Cypher5)
        ).hasErrorMessages(invalidEquijoinErrorMsg("x"))
      }
      // when the variable is already bound in the previous match clause
      test(
        s"""MATCH (x)
           |MATCH REPEATABLE ELEMENTS
           |   p1 = $selector (a)-->{1, 32}(x)-->(c),
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }
      test(
        s"""MATCH (x)
           |MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(x)-->(c)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }
    }
    allSelectiveSelectors.foreach { otherSelector =>
      // overlap between strict interior nodes of different SPPs is not okay either
      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(y)-->(c)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(
          testName,
          disabledCypherVersions = Set(CypherVersion.Cypher5)
        ).hasErrorMessages(invalidEquijoinErrorMsg("y"))
      }

      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(y)-->(y)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(
          testName,
          disabledCypherVersions = Set(CypherVersion.Cypher5)
        ).hasErrorMessages(invalidEquijoinErrorMsg("y"))
      }

      // but it is if the nodes are boundary nodes as well
      test(
        s"""MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(y),
           |   p1 = $selector (a)-->{1, 32}(y)-->(y)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }

      // Or if they are previously bound
      test(
        s"""MATCH (y)
           |MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(y)-->(c)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }
      test(
        s"""MATCH (y)
           |MATCH REPEATABLE ELEMENTS
           |   p2 = $otherSelector (x)-->{1, 32}(y)-->(z),
           |   p1 = $selector (a)-->{1, 32}(y)-->(y)
           |RETURN count(*)""".stripMargin
      ) {
        runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
      }

    }
  }

  test("(a)-[:REL]->(b), ALL PATHS (c)-[:REL]->(d)") {
    run().hasNoErrors
  }

  test("REPEATABLE ELEMENTS (a)-[:REL]->(b), ALL PATHS (c)-[:REL]->(d)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("REPEATABLE ELEMENTS shortestPath((a)-->(b))") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I39("shortestPath", 26, 1, 27),
      legacyShortestWithGpmFeaturesErrorMsg,
      p(26, 1, 27)
    )
  }

  test("DIFFERENT RELATIONSHIPS shortestPath((a)-->(b))") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I39("shortestPath", 30, 1, 31),
      legacyShortestWithGpmFeaturesErrorMsg,
      p(30, 1, 31)
    )
  }

  test("REPEATABLE ELEMENTS allShortestPaths((a)-->(b))") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I39("allShortestPaths", 26, 1, 27),
      legacyShortestWithGpmFeaturesErrorMsg,
      p(26, 1, 27)
    )
  }

  test("REPEATABLE ELEMENTS (a)-->(b) WHERE shortestPath((a)-->(b)) IS NOT NULL") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I39("shortestPath", 42, 1, 43),
      legacyShortestWithGpmFeaturesErrorMsg,
      p(42, 1, 43)
    )
  }

  test("REPEATABLE ELEMENTS (a)-->(b) WHERE EXISTS { MATCH shortestPath((a)-->(b)) }") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I39("shortestPath", 57, 1, 58),
      legacyShortestWithGpmFeaturesErrorMsg,
      p(57, 1, 58)
    )
  }

  test("CALL { MATCH REPEATABLE ELEMENTS (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN * } RETURN *") {
    run(testName, disabledVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I39("shortestPath", 49, 1, 50),
      legacyShortestWithGpmFeaturesErrorMsg,
      p(49, 1, 50)
    )
  }

  test(s"REPEATABLE ELEMENTS (a)-->(b) MATCH shortestPath((c)-->(d))") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(s"DIFFERENT RELATIONSHIPS (a)-->(b) MATCH shortestPath((c)-->(d))") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS ALL SHORTEST (a)((n1)-[r1]->(n2)){1,5}(b), ALL SHORTEST (b)-[r2]->{2,3}(c)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS ALL SHORTEST (a)((n1)-[r1:R1]->(n2)){1,5}(b)-[r2:R2]->{2,3}(c)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS (b)-[r3:R1]->(c),  ALL SHORTEST (a)((n1)-[r1:R1]->(n2)){1,5}(b)-[r2:R2]->{2,3}(b)"
  ) {
    // b is a strict interior node, but also a boundary node. Therefore, it can be used in other path-patterns.
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS ALL SHORTEST (a)((n1)-[r1:R1]->(n2)){1,2}(i)-[r2:R2]->{2,3}(b), (b)-[r3:R1]->(c), SHORTEST 1 (c)-[:R2]->{2,3}(d)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS p = ALL SHORTEST (()-[:R]-()){0,3} (b:A)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS p = ALL SHORTEST (a:A) (()-[:R]-()){0,3}"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "REPEATABLE ELEMENTS (a)--(b), ALL SHORTEST ((b)-[r2]->{2,3}(c) WHERE b.p=a.p)"
  ) {
    // selective path pattern has a predicate referring to another path pattern in the same graph pattern - reference to variable in earlier path pattern
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I21(
        java.util.List.of("a"),
        "ALL SHORTEST PATHS ((b) (()-[r2]->()){2, 3} (c) WHERE b.p = a.p)",
        79,
        1,
        80
      ),
      """From within a selective path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, `a` is defined in the same `MATCH` clause as ALL SHORTEST PATHS ((b) (()-[r2]->()){2, 3} (c) WHERE b.p = a.p).""".stripMargin,
      p(79, 1, 80)
    )
  }

  test(
    "REPEATABLE ELEMENTS ALL SHORTEST ((b)-[r2]->{2,3}(c) WHERE b.p=a.p), (a)--{,2}(b)"
  ) {
    // selective path pattern has a predicate referring to another path pattern in the same graph pattern - reference to variable in later path pattern
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I21(
        java.util.List.of("a"),
        "ALL SHORTEST PATHS ((b) (()-[r2]->()){2, 3} (c) WHERE b.p = a.p)",
        69,
        1,
        70
      ),
      """From within a selective path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, `a` is defined in the same `MATCH` clause as ALL SHORTEST PATHS ((b) (()-[r2]->()){2, 3} (c) WHERE b.p = a.p).""".stripMargin,
      p(69, 1, 70)
    )
  }

  test(
    "REPEATABLE ELEMENTS ALL SHORTEST ((b)-[r2*2..3]->(c) WHERE b.p=a.p), (a)-[*0..2]-(b)"
  ) {
    // selective path pattern has a predicate referring to another path pattern in the same graph pattern - reference to variable in later path pattern
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql42001_42I21(
        java.util.List.of("a"),
        "ALL SHORTEST PATHS ((b)-[r2*2..3]->(c) WHERE b.p = a.p)",
        69,
        1,
        70
      ),
      """From within a selective path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, `a` is defined in the same `MATCH` clause as ALL SHORTEST PATHS ((b)-[r2*2..3]->(c) WHERE b.p = a.p).""".stripMargin,
      p(69, 1, 70)
    )
  }

  test(
    "(a)--(b) MATCH REPEATABLE ELEMENTS ALL SHORTEST ((b)-[r2]->{2,3}(c) WHERE a.p=c.p)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "ALL SHORTEST (x)--{,2}(y)--{,2}(z) MATCH REPEATABLE ELEMENTS ALL SHORTEST ((b)-[r2]->{2,3}(c) WHERE b.p=y.p)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "(a)-[:R]->(e) MATCH REPEATABLE ELEMENTS (a)--(b), ALL SHORTEST ((b)--{2,3}(c)--{,2}(d) WHERE c.p=a.p)"
  ) {
    // a is already bound in a previous clause, therefore c.p=a.p is allowed here
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "(a)-[:R]->(e) MATCH REPEATABLE ELEMENTS (a)--(b), ALL SHORTEST ((b)--{2,3}(c) WHERE c.p=a.p)"
  ) {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("should accept var-length relationship without node variables inside repeatable shortest") {
    runWith(
      """MATCH REPEATABLE ELEMENTS SHORTEST 1 PATH (p = ()-[*1..1]->())
        |RETURN p""".stripMargin,
      disabledCypherVersions = Set(CypherVersion.Cypher5)
    ).hasNoErrors
  }

  test(
    "should accept subquery expression inside shortest inside REPEATABLE ELEMENTS"
  ) {
    val query =
      """MATCH REPEATABLE ELEMENTS
        |  ANY (s)( (m)-[:R]->(n)
        |    WHERE NOT EXISTS {
        |      (n)-[:S]->(:X)
        |    } ){1,10} (endPoint)
        |RETURN count(*) AS result""".stripMargin

    runWith(query, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "should accept subquery expression inside shortest inside REPEATABLE ELEMENTS with reference to previously bound variable"
  ) {
    val query =
      """  MATCH (q)
        |  MATCH REPEATABLE ELEMENTS
        |        ANY (s)( (m)-[:R]->(n)
        |        WHERE NOT EXISTS {
        |            (n)-[:S]->(q:X)
        |        } ){1,3} (endPoint)
        |  RETURN count(*) AS result""".stripMargin

    runWith(query, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    "should reject subquery expression inside shortest inside REPEATABLE ELEMENTS that references a variable defined in the same MATCH clause"
  ) {
    val shortestPathPattern = "p2 = ALL SHORTEST ((a)-->{,10}(b)-->{,10}(c) WHERE EXISTS { (b) WHERE b.p = x.p })"
    val otherPathPattern = "p1 = (x)-->{,10}(y)-->{,10}(z)"
    Seq(
      (shortestPathPattern, otherPathPattern, p(105, 2, 80)),
      (otherPathPattern, shortestPathPattern, p(140, 3, 80))
    ).foreach { case (first, second, errorPosition) =>
      val query =
        s"""MATCH REPEATABLE ELEMENTS
           |   $first,
           |   $second
           | RETURN count(*) AS result
           |""".stripMargin

      withClue(query) {

        runWith(query, disabledCypherVersions = Set(CypherVersion.Cypher5))
          .hasError(
            GqlHelper.getGql42001_42I21(
              java.util.List.of("x"),
              """p2 = ALL SHORTEST PATHS ((a) (()-->()){, 10} (b) (()-->()){, 10} (c) WHERE EXISTS {
                |  MATCH (b)
                |    WHERE b.p = x.p
                |})""".stripMargin,
              errorPosition.offset,
              errorPosition.line,
              errorPosition.column
            ),
            """From within a selective path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
              |In this case, `x` is defined in the same `MATCH` clause as p2 = ALL SHORTEST PATHS ((a) (()-->()){, 10} (b) (()-->()){, 10} (c) WHERE EXISTS {
              |  MATCH (b)
              |    WHERE b.p = x.p
              |}).""".stripMargin,
            errorPosition
          )
      }
    }
  }
}
