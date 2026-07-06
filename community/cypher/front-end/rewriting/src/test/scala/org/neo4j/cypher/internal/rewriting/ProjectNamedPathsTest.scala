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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.AstConstructionTestSupportWithPosConversion
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.rewriters.ProjectNamedPaths
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.QuantifiedPathPatternNodeInsertRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class ProjectNamedPathsTest extends CypherFunSuite with AstRewritingTestSupport
    with AstConstructionTestSupportWithPosConversion with TestName {

  private def projectionInlinedAst(queryText: String) = ast(queryText).endoRewrite(ProjectNamedPaths)

  private def projectionInlinedQppAst(queryText: String) = ast(queryText).endoRewrite(
    QuantifiedPathPatternNodeInsertRewriter.instance
  ).endoRewrite(NameAllPatternElements(new AnonymousVariableNameGenerator)).endoRewrite(ProjectNamedPaths)

  private def ast(queryText: String) = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, Some(pos))
    val parsed = parse(queryText, exceptionFactory)
    parsed.endoRewrite(inSequence(NormalizeWithAndReturnClauses(exceptionFactory, Some(CypherVersion.Cypher5))))
  }

  private def findReturnPExp(query: Statement): Expression = {
    query.asInstanceOf[SingleQuery].clauses.last.asInstanceOf[Return].returnItems.items.collectFirst {
      case AliasedReturnItem(expr, Variable("p")) => expr
    }.get
  }

  private def parseReturnedQppExpr(queryText: String) = {
    val query = projectionInlinedQppAst(queryText).asInstanceOf[Query]
    query.asInstanceOf[SingleQuery].clauses.last.asInstanceOf[Return].returnItems.items.collectFirst {
      case AliasedReturnItem(expr, Variable("p")) => expr
    }.get
  }

  private def assertRewritten(query: String, rewrittenQuery: String): Unit = {
    val rewritten = projectionInlinedAst(query)
    Prettifier(ExpressionStringifier()).asString(rewritten) shouldBe rewrittenQuery
  }

  private def assertRewrittenReturnP(query: String, rewrittenQuery: String, returnExpr: Expression): Unit = {
    val rewritten = projectionInlinedAst(query)
    Prettifier(ExpressionStringifier()).asString(rewritten) shouldBe rewrittenQuery
    findReturnPExp(rewritten) shouldBe returnExpr
  }

  test("MATCH p = (a) RETURN p") {
    assertRewrittenReturnP(
      "MATCH p = (a) RETURN p",
      """MATCH (a)
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL () {RETURN 1} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL () {
        |  RETURN 1
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL {RETURN 1} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL {
        |  RETURN 1
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL (a) {RETURN 1} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL (a) {
        |  RETURN 1
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL {WITH a RETURN 1} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL {
        |  WITH a AS a
        |  RETURN 1
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL () {RETURN 1 AS one UNION RETURN 2 as one} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL () {
        |  RETURN 1 AS one
        |  UNION
        |  RETURN 2 AS one
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL {RETURN 1 AS one UNION RETURN 2 as one} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL {
        |  RETURN 1 AS one
        |  UNION
        |  RETURN 2 AS one
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("MATCH p = (a) CALL (a) {RETURN 1 AS one UNION RETURN 2 as one} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL (a) {
        |  RETURN 1 AS one
        |  UNION
        |  RETURN 2 AS one
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )

  }

  test("MATCH p = (a) CALL {WITH a RETURN 1 AS one UNION WITH a RETURN 2 as one} RETURN p") {
    assertRewrittenReturnP(
      testName,
      """MATCH (a)
        |CALL {
        |  WITH a AS a
        |  RETURN 1 AS one
        |  UNION
        |  WITH a AS a
        |  RETURN 2 AS one
        |}
        |RETURN (a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(varFor("a"), NilPathStep()(pos))(pos)
      ) _
    )
  }

  test("Usage with redeclaration and usage later in query") {
    assertRewritten(
      """MATCH p = (a)-[r:R]->(b)
        |CALL(p) {
        |  WITH p, nodes(p) AS y
        |  CALL(p, y) {
        |    RETURN nodes(p) AS z
        |  }
        |  RETURN z
        |}
        |RETURN p, z""".stripMargin,
      """MATCH (a)-[r:R]->(b)
        |CALL (b,r,a) {
        |  WITH (a)-[r]->(b) AS p, nodes((a)-[r]->(b)) AS y
        |  CALL (p,y) {
        |    RETURN nodes(p) AS z
        |  }
        |  RETURN z AS z
        |}
        |RETURN (a)-[r]->(b) AS p, z AS z""".stripMargin
    )
  }

  test("Usage without redeclaration and usage later in query") {
    assertRewritten(
      """MATCH p = (a)-[r:R]->(b)
        |CALL(p) {
        |  WITH nodes(p) AS y
        |  CALL(p, y) {
        |    RETURN nodes(p) AS z
        |  }
        |  RETURN z
        |}
        |RETURN p AS p, z AS z""".stripMargin,
      """MATCH (a)-[r:R]->(b)
        |CALL (b,r,a) {
        |  WITH nodes((a)-[r]->(b)) AS y
        |  CALL (b,r,a,y) {
        |    RETURN nodes((a)-[r]->(b)) AS z
        |  }
        |  RETURN z AS z
        |}
        |RETURN (a)-[r]->(b) AS p, z AS z""".stripMargin
    )
  }

  test("Usage in aggregation without redeclaration and usage later in query") {
    assertRewritten(
      """MATCH p = (a)-[r:R]->(b)
        |CALL(p) {
        |  WITH count(p) AS y
        |  CALL(p, y) {
        |    RETURN nodes(p) AS z
        |  }
        |  RETURN z
        |}
        |RETURN p AS p, z AS z""".stripMargin,
      """MATCH (a)-[r:R]->(b)
        |CALL (b,r,a) {
        |  WITH count((a)-[r]->(b)) AS y
        |  CALL (b,r,a,y) {
        |    RETURN nodes((a)-[r]->(b)) AS z
        |  }
        |  RETURN z AS z
        |}
        |RETURN (a)-[r]->(b) AS p, z AS z""".stripMargin
    )
  }

  test("Redeclaration and usage in the same clause") {
    assertRewritten(
      """MATCH p = (a)-[r:R]->(b)
        |WITH p, COUNT {
        |    RETURN p AS x
        |  } AS x
        |RETURN p AS p, x AS x""".stripMargin,
      """MATCH (a)-[r:R]->(b)
        |WITH (a)-[r]->(b) AS p, COUNT { RETURN (a)-[r]->(b) AS x } AS x
        |RETURN p AS p, x AS x""".stripMargin
    )
  }

  test("CALL () {MATCH p = (a) RETURN p} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val a = varFor("a")
    val p = varFor("p")
    val CALL = {
      val MATCH = Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(MATCH, RETURN))(pos), false, Seq.empty, None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("CALL {MATCH p = (a) RETURN p} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val a = varFor("a")
    val p = varFor("p")
    val CALL = {
      val MATCH = Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(MATCH, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL (p) {RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(a), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL {WITH p RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) CALL {WITH p RETURN p AS p1} RETURN p, p1") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val p1 = varFor("p1")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  a,
                  SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
                )(pos))(pos),
                p
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(
                p,
                p1
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos),
            AliasedReturnItem(
              p1,
              p1
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  // We can assume that nameAllPatternParts has already run.
  test("MATCH p = (a)-[r]->(b) CALL (p) {RETURN p AS p1} RETURN p, p1") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val p1 = varFor("p1")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  a,
                  SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
                )(pos))(pos),
                p1
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(b, r, a), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos),
            AliasedReturnItem(
              p1,
              p1
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  // We can assume that nameAllPatternParts has already run.
  test("MATCH p = (a)-[r]->(b) CALL (p) {RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(b, r, a), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  // We can assume that nameAllPatternParts has already run.
  test("MATCH p = (a)-[r]->(b) CALL {WITH p RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  a,
                  SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
                )(pos))(pos),
                p
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b), q = (b)-[s]->(c) CALL (p, q) {RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val s = varFor("s")
    val c = varFor("c")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos),
        RelationshipChain(
          NodePattern(Some(b), None, None, None)(pos),
          RelationshipPattern(Some(s), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(c), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(b, r, a, c, s), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b), q = (b)-[s]->(c) CALL {WITH p, q RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val q = varFor("q")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val s = varFor("s")
    val c = varFor("c")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          NodePattern(Some(a), None, None, None)(pos),
          RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(b), None, None, None)(pos)
        )(pos),
        RelationshipChain(
          NodePattern(Some(b), None, None, None)(pos),
          RelationshipPattern(Some(s), None, None, None, None, SemanticDirection.OUTGOING)(pos),
          NodePattern(Some(c), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(a, a)(pos),
              AliasedReturnItem(c, c)(pos),
              AliasedReturnItem(s, s)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  a,
                  SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
                )(pos))(pos),
                p
              )(pos),
              AliasedReturnItem(
                PathExpression(NodePathStep(
                  b,
                  SingleRelationshipPathStep(s, SemanticDirection.OUTGOING, Some(c), NilPathStep()(pos))(pos)
                )(pos))(pos),
                q
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                a,
                SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, Some(b), NilPathStep()(pos))(pos)
              )(pos))(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b)<-[s]-(c) CALL (p) {RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val s = varFor("s")
    val c = varFor("c")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          RelationshipChain(
            NodePattern(Some(a), None, None, None)(pos),
            RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(b), None, None, None)(pos)
          )(pos),
          RelationshipPattern(Some(s), None, None, None, None, SemanticDirection.INCOMING)(pos),
          NodePattern(Some(c), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(s, r, b, a, c), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(
                NodePathStep(
                  a,
                  SingleRelationshipPathStep(
                    r,
                    SemanticDirection.OUTGOING,
                    Some(b),
                    SingleRelationshipPathStep(s, SemanticDirection.INCOMING, Some(c), NilPathStep()(pos))(pos)
                  )(pos)
                )(pos)
              )(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b)<-[s]-(c) CALL {WITH p RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val r = varFor("r")
    val b = varFor("b")
    val s = varFor("s")
    val c = varFor("c")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        RelationshipChain(
          RelationshipChain(
            NodePattern(Some(a), None, None, None)(pos),
            RelationshipPattern(Some(r), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(b), None, None, None)(pos)
          )(pos),
          RelationshipPattern(Some(s), None, None, None, None, SemanticDirection.INCOMING)(pos),
          NodePattern(Some(c), None, None, None)(pos)
        )(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(s, s)(pos),
              AliasedReturnItem(r, r)(pos),
              AliasedReturnItem(b, b)(pos),
              AliasedReturnItem(a, a)(pos),
              AliasedReturnItem(c, c)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(
                PathExpression(
                  NodePathStep(
                    a,
                    SingleRelationshipPathStep(
                      r,
                      SemanticDirection.OUTGOING,
                      Some(b),
                      SingleRelationshipPathStep(s, SemanticDirection.INCOMING, Some(c), NilPathStep()(pos))(pos)
                    )(pos)
                  )(pos)
                )(pos),
                p
              )(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(
                NodePathStep(
                  a,
                  SingleRelationshipPathStep(
                    r,
                    SemanticDirection.OUTGOING,
                    Some(b),
                    SingleRelationshipPathStep(s, SemanticDirection.INCOMING, Some(c), NilPathStep()(pos))(pos)
                  )(pos)
                )(pos)
              )(pos),
              p
            )(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL (p, a) {RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(a), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL {WITH p, a RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos),
              AliasedReturnItem(a, a)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a), (b) CALL (p, b) {RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val b = varFor("b")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos),
        NodePattern(Some(b), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ScopeClauseSubqueryCall(SingleQuery(List(RETURN))(pos), false, Seq(a, b), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a), (b) CALL {WITH p, b RETURN 1 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val b = varFor("b")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos),
        NodePattern(Some(b), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val WITH1 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(a, a)(pos),
              AliasedReturnItem(b, b)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None,
          withType = AddedInRewriteGeneral()
        )(pos)

      val WITH2 =
        With(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos),
              AliasedReturnItem(b, b)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None,
          None
        )(pos)

      val RETURN =
        Return(
          distinct = false,
          ReturnItems(
            FreeProjection,
            Seq(
              AliasedReturnItem(literalInt(1), one)(pos)
            )
          )(pos),
          None,
          None,
          None,
          None
        )(pos)

      ImportingWithSubqueryCall(SingleQuery(List(WITH1, WITH2, RETURN))(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL (p) {RETURN 1 AS one UNION RETURN 2 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val LEFT = {
        val RETURN =
          Return(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(literalInt(1), one)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        SingleQuery(List(RETURN))(pos)
      }
      val RIGHT = {
        val RETURN =
          Return(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(literalInt(2), one)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        SingleQuery(List(RETURN))(pos)
      }
      ScopeClauseSubqueryCall(UnionDistinct(LEFT, RIGHT)(pos), false, Seq(a), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) CALL {WITH p RETURN 1 AS one UNION WITH p RETURN 2 AS one} RETURN p") {
    val rewritten = projectionInlinedAst(testName)

    val p = varFor("p")
    val a = varFor("a")
    val one = varFor("one")

    val MATCH = Match(
      optional = false,
      matchMode = MatchMode.default(pos),
      patternForMatch(
        NodePattern(Some(a), None, None, None)(pos)
      ),
      List(),
      None,
      None
    )(pos)

    val CALL = {
      val LEFT = {
        val WITH1 =
          With(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(a, a)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None,
            None,
            withType = AddedInRewriteGeneral()
          )(pos)

        val WITH2 =
          With(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None,
            None
          )(pos)

        val RETURN =
          Return(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(literalInt(1), one)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        SingleQuery(List(WITH1, WITH2, RETURN))(pos)
      }
      val RIGHT = {
        val WITH1 =
          With(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(a, a)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None,
            None,
            withType = AddedInRewriteGeneral()
          )(pos)

        val WITH2 =
          With(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None,
            None
          )(pos)

        val RETURN =
          Return(
            distinct = false,
            ReturnItems(
              FreeProjection,
              Seq(
                AliasedReturnItem(literalInt(2), one)(pos)
              )
            )(pos),
            None,
            None,
            None,
            None
          )(pos)

        SingleQuery(List(WITH1, WITH2, RETURN))(pos)
      }
      ImportingWithSubqueryCall(UnionDistinct(LEFT, RIGHT)(pos), None, false)(pos)
    }

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, CALL, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) WITH p RETURN p") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p RETURN p")
    val a = varFor("a")
    val p = varFor("p")
    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  // don't project what is already projected
  test("MATCH p = (a) WITH p, a RETURN p") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p, a RETURN p")
    val a = varFor("a")
    val p = varFor("p")
    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos),
            AliasedReturnItem(a, a)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(p, p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a) WITH p MATCH q = (b) RETURN p, q") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH p MATCH q = (b) WITH p, q RETURN p, q")
    val a = varFor("a")
    val b = varFor("b")
    val p = varFor("p")
    val q = varFor("q")

    val MATCH1 =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val WITH1 =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos), p)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None,
        None
      )(pos)

    val MATCH2 =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(b), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val WITH2 =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(p, p)(pos),
            AliasedReturnItem(PathExpression(NodePathStep(b, NilPathStep()(pos))(pos))(pos), q)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(p, p)(pos),
            AliasedReturnItem(q, q)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH1, WITH1, MATCH2, WITH2, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p") {
    assertRewrittenReturnP(
      "MATCH p = (a)-[r]->(b) RETURN p",
      """MATCH (a)-[r]->(b)
        |RETURN (a)-[r]->(b) AS p""".stripMargin,
      PathExpression(
        NodePathStep(
          varFor("a"),
          SingleRelationshipPathStep(
            varFor("r"),
            SemanticDirection.OUTGOING,
            Some(varFor("b")),
            NilPathStep()(pos)
          )(pos)
        )(pos)
      ) _
    )
  }

  test("MATCH p = (b)<-[r]->(a) RETURN p") {
    assertRewrittenReturnP(
      "MATCH p = (b)<-[r]-(a) RETURN p",
      """MATCH (b)<-[r]-(a)
        |RETURN (b)<-[r]-(a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(
          varFor("b"),
          SingleRelationshipPathStep(
            varFor("r"),
            SemanticDirection.INCOMING,
            Some(varFor("a")),
            NilPathStep()(pos)
          )(pos)
        )(pos)
      ) _
    )
  }

  test("MATCH p = (a)-[r*]->(b) RETURN p") {
    assertRewrittenReturnP(
      "MATCH p = (a)-[r*]->(b) RETURN p",
      """MATCH (a)-[r*]->(b)
        |RETURN (a)-[r*]->(b) AS p""".stripMargin,
      PathExpression(
        NodePathStep(
          varFor("a"),
          MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
        )(pos)
      ) _
    )
  }

  test("MATCH p = (b)<-[r*]-(a) RETURN p AS p") {
    assertRewrittenReturnP(
      "MATCH p = (b)<-[r*]-(a) RETURN p AS p",
      """MATCH (b)<-[r*]-(a)
        |RETURN (b)<-[r*]-(a) AS p""".stripMargin,
      PathExpression(
        NodePathStep(
          varFor("b"),
          MultiRelationshipPathStep(varFor("r"), SemanticDirection.INCOMING, Some(varFor("a")), NilPathStep()(pos))(pos)
        )(pos)
      ) _
    )
  }

  test("MATCH p = (a) ((n)-[r]->(m)-[q]->(o))+ (b) RETURN p AS p") {
    val returns = parseReturnedQppExpr(testName)

    val expected = PathExpression(
      NodePathStep(
        varFor("a", pos),
        RepeatPathStep.asRepeatPathStep(
          List(
            varFor("n", pos),
            varFor("r", pos),
            varFor("m", pos),
            varFor("q", pos)
          ),
          varFor("b", pos),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    returns should equal(expected)
  }

  test("MATCH p = ((n)-[r]->(m)-[q]->(o))+ RETURN p AS p") {
    val returns = parseReturnedQppExpr(testName)

    val expected = PathExpression(
      NodePathStep(
        varFor("  UNNAMED0", pos),
        RepeatPathStep.asRepeatPathStep(
          List(
            varFor("n", pos),
            varFor("r", pos),
            varFor("m", pos),
            varFor("q", pos)
          ),
          varFor("  UNNAMED1", pos),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    returns should equal(expected)
  }

  test("MATCH p = (a) ((n)-[r]->(m)-[q]->(o))* ((b)-[r2]-(y))* (k) RETURN p AS p") {
    val returns = parseReturnedQppExpr(testName)

    val expected = PathExpression(
      NodePathStep(
        varFor("a", pos),
        RepeatPathStep.asRepeatPathStep(
          List(varFor("n", pos), varFor("r", pos), varFor("m", pos), varFor("q", pos)),
          varFor("  UNNAMED0", pos),
          MultiRelationshipPathStep(
            rel = v"r2",
            toNode = Some(v"k"),
            next = NilPathStep()(pos),
            direction = BOTH
          )(pos)
        )(pos)
      )(pos)
    )(pos)

    returns should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) RETURN p, 42 as order ORDER BY order")

    val aId = varFor("a")
    val orderId = varFor("order")
    val rId = varFor("r")
    val pId = varFor("p")
    val bId = varFor("b")

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          RelationshipChain(
            NodePattern(Some(aId), None, None, None)(pos),
            RelationshipPattern(Some(rId), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(bId), None, None, None)(pos)
          )(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(
              PathExpression(NodePathStep(
                aId,
                SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
              )(pos))(pos),
              pId
            )(pos),
            AliasedReturnItem(literalInt(42), orderId)(pos)
          )
        )(pos),
        None,
        Some(OrderBy(List(AscSortItem(orderId)(pos)))(pos)),
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1") {
    val rewritten = projectionInlinedAst("MATCH p = (a)-[r]->(b) WHERE length(p) > 10 RETURN 1 as x")

    val aId = varFor("a")
    val rId = varFor("r")
    val bId = varFor("b")

    val WHERE =
      Where(
        greaterThan(
          function(
            "length",
            PathExpression(NodePathStep(
              aId,
              SingleRelationshipPathStep(rId, SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(pos)
            )(pos))(pos)
          ),
          literalInt(10)
        )
      )(pos)

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          RelationshipChain(
            NodePattern(Some(aId), None, None, None)(pos),
            RelationshipPattern(Some(rId), None, None, None, None, SemanticDirection.OUTGOING)(pos),
            NodePattern(Some(bId), None, None, None)(pos)
          )(pos)
        ),
        List(),
        Some(WHERE),
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          List(
            AliasedReturnItem(literalInt(1), varFor("x"))(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("Aggregating WITH downstreams") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH length(p) as l, count(*) as x WITH l, x RETURN l + x")
    val a = varFor("a")
    val l = varFor("l")
    val x = varFor("x")
    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(a), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val pathExpression = PathExpression(NodePathStep(a, NilPathStep()(pos))(pos))(pos)
    val WITH1 =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(function("length", pathExpression), l)(pos),
            AliasedReturnItem(CountStar()(pos), x)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None,
        None
      )(pos)

    val WITH2 =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(l, l)(pos),
            AliasedReturnItem(x, x)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None,
        None
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(add(l, x), varFor("l + x"))(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH1, WITH2, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("WHERE and ORDER BY on WITH clauses should be rewritten") {
    val rewritten = projectionInlinedAst("MATCH p = (a) WITH a ORDER BY p WHERE length(p) = 1 RETURN a")

    val aId = varFor("a")

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(aId), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val pathExpression = PathExpression(NodePathStep(aId, NilPathStep()(pos))(pos))(pos)

    val WHERE =
      Where(
        equals(
          function("length", pathExpression),
          literalInt(1)
        )
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(aId, aId)(pos)
          )
        )(pos),
        None,
        Some(OrderBy(List(AscSortItem(pathExpression)(pos)))(pos)),
        None,
        None,
        Some(WHERE)
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          List(
            AliasedReturnItem(aId, aId)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("Multiple paths WHERE and ORDER BY on WITH clauses should be rewritten") {
    val rewritten = projectionInlinedAst("MATCH p = (a), q = (b) WITH p ORDER BY p WHERE length(q) = 1 RETURN p")

    val aId = varFor("a")
    val bId = varFor("b")

    val pId = varFor("p")

    val MATCH =
      Match(
        optional = false,
        matchMode = MatchMode.default(pos),
        patternForMatch(
          NodePattern(Some(aId), None, None, None)(pos),
          NodePattern(Some(bId), None, None, None)(pos)
        ),
        List(),
        None,
        None
      )(pos)

    val pathExpressionA = PathExpression(NodePathStep(aId, NilPathStep()(pos))(pos))(pos)
    val pathExpressionB = PathExpression(NodePathStep(bId, NilPathStep()(pos))(pos))(pos)

    val WHERE =
      Where(
        equals(
          function("length", pathExpressionB),
          literalInt(1)
        )
      )(pos)

    val WITH =
      With(
        distinct = false,
        ReturnItems(
          FreeProjection,
          Seq(
            AliasedReturnItem(pathExpressionA, pId)(pos)
          )
        )(pos),
        None,
        Some(OrderBy(List(AscSortItem(pId)(pos)))(pos)),
        None,
        None,
        Some(WHERE)
      )(pos)

    val RETURN =
      Return(
        distinct = false,
        ReturnItems(
          FreeProjection,
          List(
            AliasedReturnItem(pId, pId)(pos)
          )
        )(pos),
        None,
        None,
        None,
        None
      )(pos)

    val expected: Query = SingleQuery(List(MATCH, WITH, RETURN))(pos)

    rewritten should equal(expected)
  }

  test("Shortest path with predicate and path assignment, 1 relationship, OUTGOING") {
    assertRewrittenReturnP(
      "MATCH p = ANY SHORTEST ((a)-[r]->+(b) WHERE a.prop IS NOT NULL) RETURN p",
      """MATCH SHORTEST 1 PATHS ((a) (()-[r]->())+ (b) WHERE a.prop IS NOT NULL)
        |RETURN (a)-[r*]->(b) AS p""".stripMargin,
      PathExpression(step =
        NodePathStep(
          node = varFor("a"),
          next = MultiRelationshipPathStep(
            rel = v"r",
            toNode = Some(v"b"),
            next = NilPathStep()(pos),
            direction = OUTGOING
          )(pos)
        )(pos)
      )(pos)
    )
  }

  test("Shortest path with predicate and path assignment, 1 relationship, INCOMING") {
    assertRewrittenReturnP(
      "MATCH p = ANY SHORTEST ((a)<-[r]-+(b) WHERE a.prop IS NOT NULL) RETURN p",
      """MATCH SHORTEST 1 PATHS ((a) (()<-[r]-())+ (b) WHERE a.prop IS NOT NULL)
        |RETURN (a)<-[r*]-(b) AS p""".stripMargin,
      PathExpression(step =
        NodePathStep(
          node = varFor("a"),
          next = MultiRelationshipPathStep(
            rel = v"r",
            toNode = Some(v"b"),
            next = NilPathStep()(pos),
            direction = INCOMING
          )(pos)
        )(pos)
      )(pos)
    )
  }

  test("Shortest path with path assignment, 2 relationships") {
    assertRewrittenReturnP(
      "MATCH p = ANY SHORTEST ((a) ((a_in)-[r]->(b_in)-[r2]->(c_in))+ (c)) RETURN p",
      """MATCH SHORTEST 1 PATHS ((a) ((a_in)-[r]->(b_in)-[r2]->(c_in))+ (c))
        |RETURN (a) ((a_in)-[r]-(b_in)-[r2]-())* (c) AS p""".stripMargin,
      PathExpression(step =
        NodePathStep(
          node = varFor("a"),
          next = RepeatPathStep(
            variables = Seq(NodeRelPair(v"a_in", v"r"), NodeRelPair(v"b_in", v"r2")),
            toNode = varFor("c"),
            next = NilPathStep()(pos)
          )(pos)
        )(pos)
      )(pos)
    )
  }
}
