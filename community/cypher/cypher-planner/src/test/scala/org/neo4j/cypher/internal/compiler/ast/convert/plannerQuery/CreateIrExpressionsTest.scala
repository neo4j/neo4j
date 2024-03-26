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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.AddVarLengthPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.PredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.normalizePredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class CreateIrExpressionsTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  private val n = v"n"
  private val m = v"m"
  private val o = v"o"
  private val q = v"q"
  private val r = v"r"
  private val r2 = v"r2"
  private val r3 = v"r3"

  private val rPred = greaterThan(prop(r, "foo"), literalInt(5))
  private val rLessPred = lessThan(prop(r, "foo"), literalInt(10))
  private val oPred = greaterThan(prop(o, "foo"), literalInt(5))

  private val n_r_m_chain = relationshipChain(
    nodePat(Some("n")),
    relPat(Some("r"), direction = BOTH),
    nodePat(Some("m"))
  )
  private val n_r_m = RelationshipsPattern(n_r_m_chain)(pos)

  private val n_r25_m_chain: RelationshipChain = relationshipChain(
    nodePat(Some("n")),
    relPat(
      name = Some("r"),
      length = Some(Some(expressions.Range(Some(literalUnsignedInt(2)), Some(literalUnsignedInt(5)))(pos))),
      direction = BOTH
    ),
    nodePat(Some("m"))
  )

  private val n_r_m_withPreds = RelationshipsPattern(relationshipChain(
    nodePat(Some("n")),
    relPat(
      name = Some("r"),
      labelExpression = Some(labelDisjunction(labelRelTypeLeaf("R"), labelRelTypeLeaf("P"))),
      properties = Some(mapOfInt("prop" -> 5)),
      predicates = Some(rPred)
    ),
    nodePat(Some("m")),
    relPat(Some("r2"), direction = INCOMING),
    nodePat(
      Some("o"),
      Some(Negation(Wildcard()(pos))(pos)),
      Some(mapOfInt("prop" -> 5)),
      Some(oPred)
    )
  ))(pos)

  private val o_r2_m_r3_q_chain = relationshipChain(
    nodePat(Some("o")),
    relPat(Some("r2")),
    nodePat(Some("m")),
    relPat(Some("r3")),
    nodePat(Some("q"))
  )

  // { (n)-[r]->(m), (o)-[r2]->(m)-[r3]->(q) }
  private val n_r_m_r2_o_r3_q = patternForMatch(
    n_r_m_chain,
    o_r2_m_r3_q_chain
  )

  private def makeAnonymousVariableNameGenerator(): AnonymousVariableNameGenerator = new AnonymousVariableNameGenerator

  private def rewrite(e: Expression, semanticTable: SemanticTable = new SemanticTable()): Expression = {
    val anonymousVariableNameGenerator = makeAnonymousVariableNameGenerator()
    val rewriter = inSequence(
      AddUniquenessPredicates.rewriter,
      AddVarLengthPredicates.rewriter,
      normalizePredicates(PredicateNormalizer.normalizeInlinedWhereClauses),
      normalizePredicates(PredicateNormalizer.normalizeLabelAndPropertyPredicates(anonymousVariableNameGenerator)),
      flattenBooleanOperators.instance(CancellationChecker.NeverCancelled),
      CreateIrExpressions(anonymousVariableNameGenerator, semanticTable, CancellationChecker.NeverCancelled)
    )
    e.endoRewrite(rewriter)
  }

  private def queryWith(
    qg: QueryGraph,
    horizon: Option[QueryHorizon],
    tail: Option[SinglePlannerQuery] = None,
    interestingOrder: InterestingOrder = InterestingOrder.empty
  ): PlannerQuery = {
    RegularSinglePlannerQuery(
      queryGraph = qg,
      interestingOrder = interestingOrder,
      horizon = horizon.getOrElse(RegularQueryProjection()),
      tail = tail
    )
  }

  test("Rewrites Simple ExistsExpression") {
    val esc =
      simpleExistsExpression(n_r_m_r2_o_r3_q, None, MatchMode.default(pos), Set(n, m, o, q, r, r2, r3), Set.empty)

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(esc)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o, q),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r2", (o, m), OUTGOING, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r3", (m, q), OUTGOING, Seq.empty, SimplePatternLength)
            ),
          selections = Selections.from(Seq(
            differentRelationships(r, r3),
            differentRelationships(r, r2),
            differentRelationships(r3, r2)
          ))
        ),
        None
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal(
      "EXISTS { MATCH (n)-[r]-(m), (o)-[r2]->(m)-[r3]->(q)\n  WHERE NOT r = r3 AND NOT r = r2 AND NOT r3 = r2 }"
    )
  }

  test("Rewrites Simple Exists Expression with where clause") {
    val esc = simpleExistsExpression(
      n_r_m_r2_o_r3_q,
      Some(where(rPred)),
      MatchMode.default(pos),
      Set(n, r, m, r2, o, r3, q),
      Set.empty
    )

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(esc)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o, q),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r2", (o, m), OUTGOING, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r3", (m, q), OUTGOING, Seq.empty, SimplePatternLength)
            ),
          selections = Selections.from(Seq(
            differentRelationships(r, r3),
            differentRelationships(r, r2),
            differentRelationships(r3, r2),
            andedPropertyInequalities(rPred)
          ))
        ),
        None
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal(
      "EXISTS { MATCH (n)-[r]-(m), (o)-[r2]->(m)-[r3]->(q)\n  WHERE r.foo > 5 AND NOT r = r3 AND NOT r = r2 AND NOT r3 = r2 }"
    )
  }

  test("Rewrites ExistsExpression with where clause") {
    val simpleMatchQuery = singleQuery(
      match_(Seq(n_r_m_chain, o_r2_m_r3_q_chain), None),
      return_(
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, r2, r3, m, o, q)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o, q),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r2", (o, m), OUTGOING, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r3", (m, q), OUTGOING, Seq.empty, SimplePatternLength)
            ),
          selections = Selections.from(Seq(
            differentRelationships(r, r3),
            differentRelationships(r, r2),
            differentRelationships(r3, r2)
          ))
        ),
        horizon = Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal(
      "EXISTS { MATCH (n)-[r]-(m), (o)-[r2]->(m)-[r3]->(q)\n  WHERE NOT r = r3 AND NOT r = r2 AND NOT r3 = r2\nRETURN n AS n }"
    )
  }

  test("Rewrites ExistsExpression") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, m, n)), Some(Set.empty))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon = Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal("EXISTS { MATCH (n)-[r]-(m)\nRETURN n AS n }")
  }

  test("Rewrites ExistsExpression with ORDER BY") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)),
        interestingOrder =
          InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(v"n" -> n)))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal(
      "EXISTS { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING }"
    )
  }

  test("Rewrites ExistsExpression with SKIP") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        skip(2),
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final,
            queryPagination = QueryPagination(Some(literalInt(2)), None)
          ))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal("EXISTS { MATCH (n)-[r]-(m)\nRETURN n AS n\n  SKIP 2 }")
  }

  test("Rewrites ExistsExpression with LIMIT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        limit(42),
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final,
            queryPagination = QueryPagination(None, Some(literalInt(42)))
          ))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal("EXISTS { MATCH (n)-[r]-(m)\nRETURN n AS n\n  LIMIT 42 }")
  }

  test("Rewrites ExistsExpression with ORDER BY, SKIP and LIMIT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        skip(2),
        limit(42),
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final,
            queryPagination = QueryPagination(Some(literalInt(2)), Some(literalInt(42)))
          )),
        interestingOrder =
          InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(n -> n)))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal(
      "EXISTS { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING\n  SKIP 2\n  LIMIT 42 }"
    )
  }

  test("Rewrites ExistsExpression with Union") {
    val unionQuery = union(
      singleQuery(
        match_(n_r_m_chain),
        return_(
          aliasedReturnItem(n)
        )
      ),
      singleQuery(
        match_(n_r_m_chain),
        return_(
          aliasedReturnItem(n)
        )
      )
    )

    val rewrittenQuery: Query = unionQuery.endoRewrite(Namespacer.projectUnions)

    val esc = ExistsExpression(rewrittenQuery)(pos, Some(Set(r, m, n)), Some(Set.empty))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(Variable("n")(InputPosition(16, 1, 17)))

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      UnionQuery(
        RegularSinglePlannerQuery(
          QueryGraph(
            patternNodes = Set(n, m),
            patternRelationships =
              Set(
                PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
              )
          ),
          horizon = RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)
        ),
        RegularSinglePlannerQuery(
          QueryGraph(
            patternNodes = Set(n, m),
            patternRelationships =
              Set(
                PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
              )
          ),
          horizon = RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)
        ),
        distinct = true,
        List(UnionMapping(n, n, n))
      )
    )
    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal(
      "EXISTS { MATCH (n)-[r]-(m)\nRETURN n AS n\nUNION\nMATCH (n)-[r]-(m)\nRETURN n AS n }"
    )
  }

  test("Rewrites ExistsExpression with DISTINCT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      returnDistinct(
        aliasedReturnItem(n)
      )
    )

    val esc = ExistsExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val existsVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val existsIRExpression = rewritten.asInstanceOf[ExistsIRExpression]

    existsIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(DistinctQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final
          ))
      )
    )

    existsIRExpression.existsVariable shouldBe existsVariable
    existsIRExpression.solvedExpressionAsString should equal("EXISTS { MATCH (n)-[r]-(m)\nRETURN DISTINCT n AS n }")
  }

  test("Rewrites CountExpression with where clause") {
    val simpleMatchQuery = singleQuery(
      match_(Seq(n_r_m_chain, o_r2_m_r3_q_chain), None),
      return_(
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, r2, r3, m, o, q)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o, q),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r2", (o, m), OUTGOING, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r3", (m, q), OUTGOING, Seq.empty, SimplePatternLength)
            ),
          selections = Selections.from(Seq(
            differentRelationships(r, r3),
            differentRelationships(r, r2),
            differentRelationships(r3, r2)
          ))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r]-(m), (o)-[r2]->(m)-[r3]->(q)\n  WHERE NOT r = r3 AND NOT r = r2 AND NOT r3 = r2\nRETURN n AS n }"
    )
  }

  test("Rewrites CountExpression") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal("COUNT { MATCH (n)-[r]-(m)\nRETURN n AS n }")
  }

  test("Rewrites CountExpression with ORDER BY") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING }"
    )
  }

  test("Rewrites CountExpression with SKIP") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        skip(2),
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon = Some(RegularQueryProjection(
          Map(n -> n),
          queryPagination = QueryPagination(Some(literalInt(2)), None),
          position = QueryProjection.Position.Final
        )),
        tail = Some(
          RegularSinglePlannerQuery(
            horizon = AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))
          )
        )
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal("COUNT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  SKIP 2 }")
  }

  test("Rewrites CountExpression with LIMIT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        limit(42),
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon = Some(RegularQueryProjection(
          Map(n -> n),
          queryPagination = QueryPagination(None, Some(literalInt(42))),
          position = QueryProjection.Position.Final
        )),
        tail = Some(
          RegularSinglePlannerQuery(
            horizon = AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))
          )
        )
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal("COUNT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  LIMIT 42 }")
  }

  test("Rewrites CountExpression with ORDER BY, SKIP and LIMIT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        skip(2),
        limit(42),
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon = Some(RegularQueryProjection(
          Map(n -> n),
          queryPagination = QueryPagination(Some(literalInt(2)), Some(literalInt(42))),
          position = QueryProjection.Position.Final
        )),
        tail = Some(
          RegularSinglePlannerQuery(
            horizon = AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))
          )
        ),
        interestingOrder =
          InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(n -> n)))
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING\n  SKIP 2\n  LIMIT 42 }"
    )
  }

  test("Rewrites CountExpression with DISTINCT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      returnDistinct(
        aliasedReturnItem(n)
      )
    )

    val esc = CountExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon = Some(DistinctQueryProjection(
          Map(n -> n),
          position = QueryProjection.Position.Final
        )),
        tail = Some(
          RegularSinglePlannerQuery(
            horizon = AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))
          )
        )
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal("COUNT { MATCH (n)-[r]-(m)\nRETURN DISTINCT n AS n }")
  }

  test("Rewrites CountExpression with Union") {
    val unionQuery = union(
      singleQuery(
        match_(n_r_m_chain),
        return_(
          aliasedReturnItem(n)
        )
      ),
      singleQuery(
        match_(n_r_m_chain),
        return_(
          aliasedReturnItem(n)
        )
      )
    )

    val rewrittenQuery: Query = unionQuery.endoRewrite(Namespacer.projectUnions)

    val ce = CountExpression(rewrittenQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val semanticTable = new SemanticTable().addNode(Variable("n")(InputPosition(16, 1, 17)))

    val rewritten = rewrite(ce, semanticTable)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          argumentIds = Set(n)
        ),
        horizon = Some(CallSubqueryHorizon(
          callSubquery = UnionQuery(
            RegularSinglePlannerQuery(
              QueryGraph(
                patternNodes = Set(n, m),
                argumentIds = Set(n),
                patternRelationships =
                  Set(
                    PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
                  )
              ),
              horizon = RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)
            ),
            RegularSinglePlannerQuery(
              QueryGraph(
                patternNodes = Set(n, m),
                argumentIds = Set(n),
                patternRelationships =
                  Set(
                    PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
                  )
              ),
              horizon = RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)
            ),
            distinct = true,
            List(UnionMapping(n, n, n))
          ),
          correlated = true,
          yielding = true,
          inTransactionsParameters = None
        )),
        tail = Some(
          RegularSinglePlannerQuery(
            horizon = AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))
          )
        )
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r]-(m)\nRETURN n AS n\nUNION\nMATCH (n)-[r]-(m)\nRETURN n AS n }"
    )
  }

  test("should rewrite COUNT { (n)-[r]-(m) }") {
    val p = patternForMatch(n_r_m.element)
    val countExpr = simpleCountExpression(p, None, MatchMode.default(pos), Set(m, r), Set(n))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(countExpr)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(PatternRelationship(r, (n, m), BOTH, Seq.empty, SimplePatternLength))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString shouldBe s"COUNT { MATCH (n)-[r]-(m) }"
  }

  test("should rewrite COUNT { (n)-[r]-(m) WHERE r.foo > 5}") {
    val p = patternForMatch(n_r_m.element)
    val countExpr = simpleCountExpression(p, Some(where(rPred)), MatchMode.default(pos), Set(m, r), Set(n))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(countExpr)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(PatternRelationship(r, (n, m), BOTH, Seq.empty, SimplePatternLength)),
          selections = Selections.from(andedPropertyInequalities(rPred))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal("COUNT { MATCH (n)-[r]-(m)\n  WHERE r.foo > 5 }")
  }

  test("should rewrite count expression with longer pattern and inlined predicates") {
    val p = patternForMatch(n_r_m_withPreds.element)
    val countExpr = simpleCountExpression(p, None, MatchMode.default(pos), Set(m, r), Set(n))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(countExpr)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o),
          argumentIds = Set(n),
          patternRelationships = Set(
            PatternRelationship(
              r,
              (n, m),
              OUTGOING,
              Seq(relTypeName("R"), relTypeName("P")),
              SimplePatternLength
            ),
            PatternRelationship(r2, (m, o), INCOMING, Seq.empty, SimplePatternLength)
          ),
          selections = Selections.from(Seq(
            differentRelationships(r2, r),
            andedPropertyInequalities(rPred),
            andedPropertyInequalities(oPred),
            equals(prop(r, "prop"), literalInt(5)),
            equals(prop(o, "prop"), literalInt(5)),
            not(hasALabel(o.name))
          ))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r:R|P]->(m)<-[r2]-(o)\n  WHERE r.prop = 5 AND o.prop = 5 AND NOT o:% AND r.foo > 5 AND o.foo > 5 AND NOT r2 = r }"
    )
  }

  test("should rewrite COUNT { (m) } and add a type check") {
    val p = patternForMatch(n_r_m.element.rightNode)
    val countExpr = simpleCountExpression(p, None, MatchMode.default(pos), Set(n, r), Set(m))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(countExpr)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(m),
          argumentIds = Set(m),
          selections = Selections.from(AssertIsNode(m)(pos))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal("COUNT { MATCH (m) }")
  }

  test("should rewrite COUNT { (n)-[r]-(m) WHERE r.foo > 5 AND r.foo < 10 } and group predicates") {
    val p = patternForMatch(n_r_m.element)
    val countExpr =
      simpleCountExpression(p, Some(where(and(rPred, rLessPred))), MatchMode.default(pos), Set(m, r), Set(n))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(countExpr)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(PatternRelationship(r, (n, m), BOTH, Seq.empty, SimplePatternLength)),
          selections = Selections.from(andedPropertyInequalities(rPred, rLessPred))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r]-(m)\n  WHERE r.foo > 5 AND r.foo < 10 }"
    )
  }

  test("Should rewrite COUNT { (n)-[r]->(m), (o)-[r2]->(m)-[r3]->(q) }") {
    val countExpr =
      simpleCountExpression(n_r_m_r2_o_r3_q, None, MatchMode.default(pos), Set(r, m, r2, r3, q), Set(n, o))

    val nameGenerator = makeAnonymousVariableNameGenerator()
    val countVariable = varFor(nameGenerator.nextName)

    val rewritten = rewrite(countExpr)
    val countIRExpression = rewritten.asInstanceOf[CountIRExpression]

    countIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o, q),
          argumentIds = Set(n, o),
          patternRelationships = Set(
            PatternRelationship(r, (n, m), BOTH, Seq.empty, SimplePatternLength),
            PatternRelationship(r2, (o, m), OUTGOING, Seq.empty, SimplePatternLength),
            PatternRelationship(r3, (m, q), OUTGOING, Seq.empty, SimplePatternLength)
          ),
          selections = Selections.from(Seq(
            differentRelationships(r, r2),
            differentRelationships(r, r3),
            differentRelationships(r3, r2)
          ))
        ),
        horizon =
          Some(AggregatingQueryProjection(aggregationExpressions = Map(countVariable -> CountStar()(pos)))),
        None
      )
    )

    countIRExpression.countVariable shouldBe countVariable
    countIRExpression.solvedExpressionAsString should equal(
      "COUNT { MATCH (n)-[r]-(m), (o)-[r2]->(m)-[r3]->(q)\n  WHERE NOT r = r3 AND NOT r = r2 AND NOT r3 = r2 }"
    )
  }

  test("Rewrites CollectExpression") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        aliasedReturnItem(n)
      )
    )

    val ce = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(ce, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)),
        None
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal("COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n }")
  }

  test("Rewrites CollectExpression with varlength relationship") {
    val simpleMatchQuery = singleQuery(
      match_(n_r25_m_chain),
      return_(
        aliasedReturnItem(n)
      )
    )

    val ce = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(ce, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(PatternRelationship(r, (n, m), BOTH, Seq.empty, VarPatternLength(2, Some(5)))),
          selections = Selections.from(Seq(
            unique(v"r"),
            varLengthLowerLimitPredicate("r", 2),
            varLengthUpperLimitPredicate("r", 5)
          ))
        ),
        horizon =
          Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)),
        None
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r*2..5]-(m)\n  WHERE unique(r) AND size(r) >= 2 AND size(r) <= 5\nRETURN n AS n }"
    )
  }

  test("Rewrites Ordered CollectExpression") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        aliasedReturnItem(n)
      )
    )

    val ce = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(ce, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)),
        None,
        InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(n -> n)))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING }"
    )
  }

  test("Rewrites CollectExpression with WHERE") {
    val ce = simpleCollectExpression(
      n_r_m_r2_o_r3_q,
      Some(where(rPred)),
      return_(
        orderBy(n.asc),
        aliasedReturnItem(n)
      ),
      MatchMode.default(pos),
      Set(n, r, m, r2, o, r3, q),
      Set.empty
    )

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(ce, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m, o, q),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r2", (o, m), OUTGOING, Seq.empty, SimplePatternLength),
              PatternRelationship(v"r3", (m, q), OUTGOING, Seq.empty, SimplePatternLength)
            ),
          selections = Selections.from(Seq(
            differentRelationships(r, r3),
            differentRelationships(r, r2),
            differentRelationships(r3, r2),
            andedPropertyInequalities(rPred)
          ))
        ),
        horizon =
          Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)),
        None,
        InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(n -> n)))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m), (o)-[r2]->(m)-[r3]->(q)\n  WHERE r.foo > 5 AND NOT r = r3 AND NOT r = r2 AND NOT r3 = r2\nRETURN n AS n\n  ORDER BY n ASCENDING }"
    )
  }

  test("Rewrites CollectExpression with ORDER BY") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        aliasedReturnItem(n)
      )
    )

    val esc = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)),
        interestingOrder =
          InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(n -> n)))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING }"
    )
  }

  test("Rewrites CollectExpression with SKIP") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        skip(2),
        aliasedReturnItem(n)
      )
    )

    val esc = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final,
            queryPagination = QueryPagination(Some(literalInt(2)), None)
          ))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal("COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  SKIP 2 }")
  }

  test("Rewrites CollectExpression with LIMIT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        limit(42),
        aliasedReturnItem(n)
      )
    )

    val esc = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final,
            queryPagination = QueryPagination(None, Some(literalInt(42)))
          ))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  LIMIT 42 }"
    )
  }

  test("Rewrites CollectExpression with ORDER BY, SKIP and LIMIT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      return_(
        orderBy(n.asc),
        skip(2),
        limit(42),
        aliasedReturnItem(n)
      )
    )

    val esc = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(RegularQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final,
            queryPagination = QueryPagination(Some(literalInt(2)), Some(literalInt(42)))
          )),
        interestingOrder =
          InterestingOrder.required(RequiredOrderCandidate.asc(n, Map(n -> n)))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n\n  ORDER BY n ASCENDING\n  SKIP 2\n  LIMIT 42 }"
    )
  }

  test("Rewrites CollectExpression with DISTINCT") {
    val simpleMatchQuery = singleQuery(
      match_(n_r_m_chain),
      returnDistinct(
        aliasedReturnItem(n)
      )
    )

    val esc = CollectExpression(simpleMatchQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val collectVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(n)

    val rewritten = rewrite(esc, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      queryWith(
        QueryGraph(
          patternNodes = Set(n, m),
          argumentIds = Set(n),
          patternRelationships =
            Set(
              PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
            )
        ),
        horizon =
          Some(DistinctQueryProjection(
            Map(n -> n),
            position = QueryProjection.Position.Final
          ))
      )
    )

    collectIRExpression.collection shouldBe collectVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m)\nRETURN DISTINCT n AS n }"
    )
  }

  test("Rewrites Collect Expression with Union") {
    val unionQuery = union(
      singleQuery(
        match_(n_r_m_chain),
        return_(
          aliasedReturnItem(n)
        )
      ),
      singleQuery(
        match_(n_r_m_chain),
        return_(
          aliasedReturnItem(n)
        )
      )
    )

    val rewrittenQuery: Query = unionQuery.endoRewrite(Namespacer.projectUnions)

    val ce = CollectExpression(rewrittenQuery)(pos, Some(Set(r, m)), Some(Set(n)))

    val countVariable = varFor(makeAnonymousVariableNameGenerator().nextName)
    val semanticTable = new SemanticTable().addNode(Variable("n")(InputPosition(16, 1, 17)))

    val rewritten = rewrite(ce, semanticTable)
    val collectIRExpression = rewritten.asInstanceOf[ListIRExpression]

    collectIRExpression.query should equal(
      UnionQuery(
        RegularSinglePlannerQuery(
          QueryGraph(
            patternNodes = Set(n, m),
            argumentIds = Set(n),
            patternRelationships =
              Set(
                PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
              )
          ),
          horizon = RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)
        ),
        RegularSinglePlannerQuery(
          QueryGraph(
            patternNodes = Set(n, m),
            argumentIds = Set(n),
            patternRelationships =
              Set(
                PatternRelationship(v"r", (n, m), BOTH, Seq.empty, SimplePatternLength)
              )
          ),
          horizon = RegularQueryProjection(Map(n -> n), position = QueryProjection.Position.Final)
        ),
        distinct = true,
        List(UnionMapping(n, n, n))
      )
    )

    collectIRExpression.collection shouldBe countVariable
    collectIRExpression.variableToCollect shouldBe n
    collectIRExpression.solvedExpressionAsString should equal(
      "COLLECT { MATCH (n)-[r]-(m)\nRETURN n AS n\nUNION\nMATCH (n)-[r]-(m)\nRETURN n AS n }"
    )
  }

}
