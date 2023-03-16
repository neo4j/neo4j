/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class RemoveUnusedNamedGroupVariablesPhaseTest extends CypherFunSuite with AstConstructionTestSupport with TestName
    with RewritePhaseTest {

  override def rewriterPhaseUnderTest: Phase[BaseContext, BaseState, BaseState] = RemoveUnusedNamedGroupVariablesPhase

  override def preProcessPhase(features: SemanticFeature*): Transformer[BaseContext, BaseState, BaseState] =
    SemanticAnalysis(
      warn = true,
      features: _*
    ) andThen
      Namespacer andThen
      ProjectNamedPathsRewriter

  test("MATCH (a) ((n)-[r]->(m)-[q]->(o))+ RETURN o,m") {
    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  r@1"), varFor("  r@6"))(pos),
      VariableGrouping(varFor("  q@3"), varFor("  q@7"))(pos),
      VariableGrouping(varFor("  m@2"), varFor("  m@5"))(pos),
      VariableGrouping(varFor("  o@4"), varFor("  o@9"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("MATCH (a) (()-[r]->(m)-[q]->())+ RETURN q") {
    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  r@0"), varFor("  r@3"))(pos),
      VariableGrouping(varFor("  q@2"), varFor("  q@5"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("MATCH (a) (()-[r]->(m)-[q]->())+ RETURN *") {
    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  r@0"), varFor("  r@3"))(pos),
      VariableGrouping(varFor("  m@1"), varFor("  m@4"))(pos),
      VariableGrouping(varFor("  q@2"), varFor("  q@5"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("MATCH (a) ((n)-[r]->(m)-[q]->(o))+ (b) RETURN n,r,m") {

    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  m@2"), varFor("  m@5"))(pos),
      VariableGrouping(varFor("  n@0"), varFor("  n@8"))(pos),
      VariableGrouping(varFor("  r@1"), varFor("  r@6"))(pos),
      VariableGrouping(varFor("  q@3"), varFor("  q@7"))(pos)
    )

    rewrittenQpp.variableGroupings should be(expectedVariableGroupings)
  }

  test("MATCH p = (a) (()-[r]->(m)-[q]->())+ (b) RETURN p") {
    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  m@1"), varFor("  m@4"))(pos),
      VariableGrouping(varFor("  r@0"), varFor("  r@3"))(pos),
      VariableGrouping(varFor("  q@2"), varFor("  q@5"))(pos),
      VariableGrouping(varFor("  UNNAMED0"), varFor("  UNNAMED0"))(pos)
    )

    rewrittenQpp.variableGroupings should be(expectedVariableGroupings)
  }

  test("MATCH (a) (()-[r]->(m)-[q]->() WHERE m.prop = 5)+ RETURN 1") {
    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  r@0"), varFor("  r@3"))(pos),
      VariableGrouping(varFor("  q@2"), varFor("  q@5"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("MATCH (a) (()-[r]->(m)-[q]->())+ WHERE m IS NOT NULL RETURN 1") {
    val statement = prepareFrom(
      testName,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  m@1"), varFor("  m@4"))(pos),
      VariableGrouping(varFor("  r@0"), varFor("  r@3"))(pos),
      VariableGrouping(varFor("  q@2"), varFor("  q@5"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables in subqueries") {
    val query = """MATCH (c)-->(d)
                  |  CALL {
                  |    MATCH ((a)-[r]->(b))+
                  |    WITH a
                  |    RETURN a
                  |  }
                  |  RETURN 1""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@3"))(pos),
      VariableGrouping(varFor("  r@1"), varFor("  r@4"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables in path") {
    val query = """MATCH p = ((a)-->())+ RETURN 1""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in UNWIND") {
    val query = """MATCH ((a)-->())+ UNWIND a AS foo RETURN 1""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in WITH") {
    val query = """MATCH ((a)-->())+ WITH a AS foo RETURN 1 """.stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in IMPORT-WITH") {
    val query = """MATCH ((a)-->())+ CALL { WITH a RETURN 1 AS n } RETURN n""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in ORDER BY") {
    val query = """MATCH ((a)-->())+ RETURN 1 ORDER BY a""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in FOREACH") {
    val query = """MATCH ((a)-->())+ FOREACH (_a in a | SET _a.n = 1) RETURN 1""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in another MATCH") {
    val query = """MATCH ((a)-->())+ MATCH (c WHERE c = a) RETURN 1""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in procedure argument") {
    val query = """MATCH ((a)-->())+ RETURN abs(a[0].n)""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in DELETE") {
    val query = """MATCH ((a)-->())+ DELETE a[0]""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in CREATE") {
    val query = """MATCH ((a)-->())+ CREATE (x {prop: a[0].prop})""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in SET") {
    val query = """MATCH ((a)-->())+ SET (a[0]).prop = 5""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }

  test("Should remove unnecessary qpp group variables with variable reference in REMOVE") {
    val query = """MATCH ((a)-->())+ REMOVE (a[0]).prop""".stripMargin

    val statement = prepareFrom(
      query,
      rewriterPhaseUnderTest,
      SemanticFeature.QuantifiedPathPatterns
    ).statement()

    val rewrittenQpp = statement.folder.treeFindByClass[QuantifiedPath].get

    val expectedVariableGroupings = Set(
      VariableGrouping(varFor("  a@0"), varFor("  a@2"))(pos),
      VariableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED3"))(pos)
    )

    rewrittenQpp.variableGroupings should equal(expectedVariableGroupings)
  }
}
