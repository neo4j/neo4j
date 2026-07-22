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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandClauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ComputeExpressionDependencies
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Asserts that the WorkingScope-sourced [[ComputeExpressionDependencies]] phase produces the same
 * `computedIntroducedVariables` / `computedScopeDependencies` for every
 * [[ExpressionWithComputedDependencies]] as the legacy SemanticState-based
 * [[computeDependenciesForExpressions]] rewriter.
 *
 * Subclasses vary the preparation pipeline so parity is checked both before and after
 * NameAllPatternElements (part of [[AstRewriting]]) names anonymous pattern elements.
 */
abstract class ComputeExpressionDependenciesParityTestBase extends CypherFunSuite
    with AstConstructionTestSupport
    with RewritePhaseTest {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] = NoOp()

  private type Fields = (Set[String], Set[String])

  private def fieldsOf(statement: Statement): Seq[Fields] =
    statement.folder.treeCollect {
      case ecd: ExpressionWithComputedDependencies =>
        (
          ecd.introducedVariables.map(_.name),
          ecd.scopeDependencies.map(_.name)
        )
    }

  private def assertParity(query: String, version: CypherVersion = CypherVersion.Cypher25): Unit = {
    val state = prepareFrom(version, query, rewriterPhaseUnderTest)

    val oldStatement = state.statement().endoRewrite(computeDependenciesForExpressions(state.semantics()))

    val context = ContextHelper.create(version, query, phaseTestConfig.semanticFeatures, databaseReference)
    val newStatement = ComputeExpressionDependencies.process(state, context).statement()

    val oldFields = fieldsOf(oldStatement)
    val newFields = fieldsOf(newStatement)

    withClue(s"CYPHER $version\nQuery: $query\n") {
      newFields.size shouldEqual oldFields.size
      newFields.zip(oldFields).zipWithIndex.foreach { case ((actual, expected), i) =>
        withClue(s"ExpressionWithComputedDependencies #$i introduced/scopeDeps mismatch\n") {
          actual shouldEqual expected
        }
      }
    }
  }

  protected def corpus: Seq[String] = Seq(
    // EXISTS / COUNT / COLLECT full subqueries, correlated and uncorrelated
    "MATCH (a) WHERE EXISTS { MATCH (b) WHERE b.p = a.p RETURN b } RETURN a",
    "MATCH (a) RETURN COUNT { MATCH (b)-[:R]->(a) RETURN b } AS c",
    "MATCH (a) RETURN COLLECT { MATCH (b) WHERE b.p = a.p RETURN b.p } AS c",
    "MATCH (a) WHERE EXISTS { MATCH (b) RETURN b } RETURN a",
    // Pattern expression and pattern comprehension (may contain anonymous variables)
    "MATCH (n) WHERE (n)-->() RETURN n",
    "MATCH (n) RETURN [(n)-->(m) | m.p] AS ps",
    "MATCH (n) WHERE (n)-[:R]->(:L) RETURN n",
    // Correlated referencing an outer variable from inside a pattern comprehension
    "MATCH (a) RETURN [(a)-->(m) WHERE m.p = a.p | m] AS ms",
    // Nested subquery expressions
    "MATCH (a) RETURN COLLECT { MATCH (b) RETURN COLLECT { MATCH (c) WHERE c.p = b.p RETURN c.p } } AS r",
    "MATCH (a) WHERE EXISTS { MATCH (b) WHERE EXISTS { MATCH (c) WHERE c.p = b.p RETURN c } RETURN b } RETURN a",
    "MATCH (dog)<--({canAffordDog: EXISTS { WITH toBoolean(sum(0)) AS n1, dog AS dog RETURN 0 }}) RETURN dog.name AS name",
    "CREATE (a)-[r:T]->(b) CREATE (c {p: COUNT { WITH r ORDER BY r } })"
  )

  test("WorkingScope-sourced dependencies match the legacy derivation across a corpus") {
    CypherVersion.values().foreach(version => corpus.foreach(q => assertParity(q, version)))
  }

  test("QPP singleton/group: nested EXISTS introduced vars bind to the body (inward) facing") {
    val query =
      """MATCH (movie:Movie)
        |WHERE movie.imdbId=$imdbId
        |MATCH (movie)<-[:LIKES]-(user:User)-[:LIVES_NEAR]->(place:ReferencePlace)
        |MATCH (user)( (:User)-[:FRIEND]-(u:User) WHERE EXISTS {
        |                                                        (u)((:User)-[:DATING]-(:User)){1,5}()-[:LIVES_NEAR]->(place)
        |                                                      } ){0,2}(friend:User)
        |RETURN DISTINCT friend""".stripMargin

    assertParity(query)
  }

  test("nested COLLECT: outer introducedVariables captures recursively-inner declarations") {
    val query = "MATCH (a) RETURN COLLECT { MATCH (b) RETURN COLLECT { MATCH (c) RETURN c } } AS r"
    val state = prepareFrom(CypherVersion.Cypher25, query, rewriterPhaseUnderTest)
    val context =
      ContextHelper.create(CypherVersion.Cypher25, query, phaseTestConfig.semanticFeatures, databaseReference)
    val newStatement = ComputeExpressionDependencies.process(state, context).statement()

    val outer = newStatement.folder.treeCollect {
      case ecd: ExpressionWithComputedDependencies => ecd
    }.head
    val names = outer.introducedVariables.map(_.name)
    withClue(s"outer introducedVariables = $names\n") {
      names should contain("b")
      names should contain("c")
    }
  }
}

/** Parity BEFORE pattern elements are named (NameAllPatternElements has not run). */
class ComputeExpressionDependenciesTest extends ComputeExpressionDependenciesParityTestBase {

  override def preProcessTransformer: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen SemanticAnalysis(Some(false))

  test("pattern comprehension: outer dependency resolves to a different (but equivalent) occurrence than legacy") {
    val query = "MATCH (a) RETURN [(a)-->(m) WHERE m.p = a.p | m] AS ms"
    val state = prepareFrom(CypherVersion.Cypher25, query, rewriterPhaseUnderTest)
    val legacyEcd = ecdOf(state.statement().endoRewrite(computeDependenciesForExpressions(state.semantics())))
    val context =
      ContextHelper.create(CypherVersion.Cypher25, query, phaseTestConfig.semanticFeatures, databaseReference)
    val workingScopeEcd = ecdOf(ComputeExpressionDependencies.process(state, context).statement())

    workingScopeEcd.scopeDependencies.map(_.name) shouldEqual Set("a")
    legacyEcd.scopeDependencies.map(_.name) shouldEqual Set("a")
    workingScopeEcd.introducedVariables.map(v => (v.name, v.position.offset)) shouldEqual
      legacyEcd.introducedVariables.map(v => (v.name, v.position.offset))

    val legacyA = legacyEcd.scopeDependencies.find(_.name == "a").get
    val workingScopeA = workingScopeEcd.scopeDependencies.find(_.name == "a").get
    withClue(s"legacy a@${legacyA.position.offset}, workingScope a@${workingScopeA.position.offset}: ") {
      legacyA.position.offset shouldEqual query.indexOf("(a)-->(m)") + 1
      workingScopeA.position.offset shouldEqual query.indexOf("a.p")
    }
  }

  private def ecdOf(statement: Statement): ExpressionWithComputedDependencies =
    statement.folder.treeCollect { case e: ExpressionWithComputedDependencies => e }.head
}

/** Parity after NameAllPatternElements (ASTRewriting) names anonymous pattern elements. */
class ComputeExpressionDependenciesPostNamingTest extends ComputeExpressionDependenciesParityTestBase {

  override protected def corpus: Seq[String] = Seq(
    "MATCH (a) RETURN COUNT { MATCH (b)-[:R]->(a) RETURN b } AS c",
    "MATCH (a) WHERE EXISTS { MATCH (b)-->(a) RETURN b } RETURN a",
    "MATCH (a) RETURN COLLECT { MATCH (b) WHERE b.p = a.p RETURN b.p } AS c",
    "MATCH (a) WHERE EXISTS { MATCH (b) RETURN b } RETURN a",
    "MATCH (a) RETURN COLLECT { MATCH (b) RETURN COLLECT { MATCH (c) WHERE c.p = b.p RETURN c.p } } AS r",
    "MATCH (a) WHERE EXISTS { MATCH (b)-[:R]->() WHERE EXISTS { MATCH (c)-->(b) RETURN c } RETURN b } RETURN a"
  )

  override def preProcessTransformer: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen
      ExpandClauses andThen
      RewritePhaseTest.reanalyze andThen
      AstRewriting() andThen
      RewritePhaseTest.reanalyze andThen
      ScopeSurveyor
}
