/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelExpression.Disjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.rewriting.rewriters.inlineNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CreateIrExpressionsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val n = varFor("n")
  private val m = varFor("m")
  private val o = varFor("o")
  private val r = varFor("r")
  private val r2 = varFor("r2")
  private val path = varFor("p")
  private val variableToCollectName = "c"
  private val collectionName = "col"

  private val rPred = greaterThan(prop(r.name, "foo"), literalInt(5))
  private val oPred = greaterThan(prop(o.name, "foo"), literalInt(5))

  private val n_r_m = RelationshipsPattern(RelationshipChain(
    NodePattern(Some(n), None, None, None)(pos),
    RelationshipPattern(Some(r), None, None, None, None, BOTH)(pos),
    NodePattern(Some(m), None, None, None)(pos)
  )(pos))(pos)

  private val n_r_m_withLabelDisjunction = RelationshipsPattern(RelationshipChain(
    NodePattern(Some(n), None, None, None)(pos),
    RelationshipPattern(Some(r), None, None, None, None, BOTH)(pos),
    NodePattern(Some(m), Some(Disjunction(Leaf(labelName("M")), Leaf(labelName("MM")))(pos)), None, None)(pos)
  )(pos))(pos)

  private val n_r25_m = RelationshipsPattern(RelationshipChain(
    NodePattern(Some(n), None, None, None)(pos),
    RelationshipPattern(
      Some(r),
      None,
      Some(Some(expressions.Range(Some(literalUnsignedInt(2)), Some(literalUnsignedInt(5)))(pos))),
      None,
      None,
      BOTH
    )(pos),
    NodePattern(Some(m), None, None, None)(pos)
  )(pos))(pos)

  private val n_r_m_withPreds = RelationshipsPattern(RelationshipChain(
    RelationshipChain(
      NodePattern(Some(n), None, None, None)(pos),
      RelationshipPattern(
        Some(r),
        Some(Disjunction(Leaf(relTypeName("R")), Leaf(relTypeName("P")))(pos)),
        None,
        Some(mapOfInt("prop" -> 5)),
        Some(rPred),
        OUTGOING
      )(pos),
      NodePattern(Some(m), None, None, None)(pos)
    )(pos),
    RelationshipPattern(Some(r2), None, None, None, None, INCOMING)(pos),
    NodePattern(
      Some(o),
      Some(Negation(Wildcard()(pos))(pos)),
      Some(mapOfInt("prop" -> 5)),
      Some(oPred)
    )(pos)
  )(pos))(pos)

  private def rewrite(e: Expression): Expression = {
    val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator
    val rewriter = inSequence(
      inlineNamedPathsInPatternComprehensions.instance,
      CreateIrExpressions(anonymousVariableNameGenerator)
    )
    e.endoRewrite(rewriter)
  }

  private def queryWith(qg: QueryGraph, horizon: Option[QueryHorizon]): PlannerQuery = {
    PlannerQuery(
      RegularSinglePlannerQuery(
        queryGraph = qg,
        horizon = horizon.getOrElse(RegularQueryProjection()),
        tail = None
      )
    )
  }

  test("Rewrites PatternExpression") {
    val pe = PatternExpression(n_r_m)(Set(n), variableToCollectName, collectionName)
    val pathExpression = PathExpressionBuilder.node(n.name).bothTo(r.name, m.name).build()

    val rewritten = rewrite(pe)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name),
            argumentIds = Set(n.name),
            patternRelationships =
              Set(PatternRelationship(r.name, (n.name, m.name), BOTH, Seq.empty, SimplePatternLength))
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> pathExpression)))
        ),
        variableToCollectName,
        collectionName,
        s"(${n.name})-[${r.name}]-(${m.name})"
      )(pos)
    )
  }

  test("Rewrites PatternExpression with varlength relationship") {
    val pe = PatternExpression(n_r25_m)(Set(n), variableToCollectName, collectionName)
    val pathExpression = PathExpressionBuilder.node(n.name).bothToVarLength(r.name, m.name).build()

    val rewritten = rewrite(pe)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name),
            argumentIds = Set(n.name),
            patternRelationships =
              Set(PatternRelationship(r.name, (n.name, m.name), BOTH, Seq.empty, VarPatternLength(2, Some(5))))
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> pathExpression)))
        ),
        variableToCollectName,
        collectionName,
        s"(${n.name})-[${r.name}*2..5]-(${m.name})"
      )(pos)
    )
  }

  test("Rewrites PatternExpression with longer pattern and inlined predicates") {
    val pe = PatternExpression(n_r_m_withPreds)(Set(n), variableToCollectName, collectionName)
    val pathExpression = PathExpressionBuilder.node(n.name).outTo(r.name, m.name).inTo(r2.name, o.name).build()

    val rewritten = rewrite(pe)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name, o.name),
            argumentIds = Set(n.name),
            patternRelationships = Set(
              PatternRelationship(
                r.name,
                (n.name, m.name),
                OUTGOING,
                Seq(relTypeName("R"), relTypeName("P")),
                SimplePatternLength
              ),
              PatternRelationship(r2.name, (m.name, o.name), INCOMING, Seq.empty, SimplePatternLength)
            ),
            selections = Selections.from(Seq(
              not(equals(r, r2)),
              rPred,
              oPred,
              equals(prop(r.name, "prop"), literalInt(5)),
              equals(prop(o.name, "prop"), literalInt(5)),
              not(hasALabel(o.name))
            ))
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> pathExpression)))
        ),
        variableToCollectName,
        collectionName,
        s"(${n.name})-[${r.name}:R|P {prop: 5} WHERE ${r.name}.foo > 5]->(${m.name})<-[${r2.name}]-(${o.name}:!% {prop: 5} WHERE ${o.name}.foo > 5)"
      )(pos)
    )
  }

  test("Rewrites exists(PatternExpression) with Node label disjunction") {
    val pe = PatternExpression(n_r_m_withLabelDisjunction)(Set(n), variableToCollectName, collectionName)

    val rewritten = rewrite(Exists(pe)(pos))

    rewritten should equal(
      ExistsIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name),
            argumentIds = Set(n.name),
            patternRelationships =
              Set(PatternRelationship(r.name, (n.name, m.name), BOTH, Seq.empty, SimplePatternLength)),
            selections = Selections.from(ors(hasLabels(m, "M"), hasLabels(m, "MM")))
          ),
          None
        ),
        s"exists((${n.name})-[${r.name}]-(${m.name}:M|MM))"
      )(pos)
    )
  }

  test("Rewrites PatternComprehension") {
    val pc = PatternComprehension(
      None,
      n_r_m,
      None,
      literalInt(5)
    )(pos, Set(n), variableToCollectName, collectionName)

    val rewritten = rewrite(pc)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name),
            argumentIds = Set(n.name),
            patternRelationships =
              Set(PatternRelationship(r.name, (n.name, m.name), BOTH, Seq.empty, SimplePatternLength))
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> literalInt(5))))
        ),
        variableToCollectName,
        collectionName,
        s"[(${n.name})-[${r.name}]-(${m.name}) | 5]"
      )(pos)
    )
  }

  test("Rewrites PatternComprehension with named path") {
    val pc = PatternComprehension(
      Some(path),
      n_r_m,
      None,
      path
    )(pos, Set(n), variableToCollectName, collectionName)

    val pathExpression = PathExpressionBuilder.node(n.name).bothTo(r.name, m.name).build()

    val rewritten = rewrite(pc)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name),
            argumentIds = Set(n.name),
            patternRelationships =
              Set(PatternRelationship(r.name, (n.name, m.name), BOTH, Seq.empty, SimplePatternLength))
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> pathExpression)))
        ),
        variableToCollectName,
        collectionName,
        s"[(${n.name})-[${r.name}]-(${m.name}) | (${n.name})-[${r.name}]-(${m.name})]"
      )(pos)
    )
  }

  test("Rewrites PatternComprehension with WHERE clause") {
    val rPred = greaterThan(prop(r.name, "foo"), literalInt(5))

    val pc = PatternComprehension(
      None,
      n_r_m,
      Some(rPred),
      prop(m, "foo")
    )(pos, Set(n), variableToCollectName, collectionName)

    val rewritten = rewrite(pc)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name),
            argumentIds = Set(n.name),
            patternRelationships =
              Set(PatternRelationship(r.name, (n.name, m.name), BOTH, Seq.empty, SimplePatternLength)),
            selections = Selections.from(rPred)
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> prop(m, "foo"))))
        ),
        variableToCollectName,
        collectionName,
        s"[(${n.name})-[${r.name}]-(${m.name}) WHERE ${r.name}.foo > 5 | ${m.name}.foo]"
      )(pos)
    )
  }

  test("Rewrites PatternComprehension with longer pattern and inlined predicates") {
    val rPred = greaterThan(prop(r.name, "foo"), literalInt(5))
    val oPred = greaterThan(prop(o.name, "foo"), literalInt(5))

    val pc = PatternComprehension(
      None,
      n_r_m_withPreds,
      None,
      literalInt(5)
    )(pos, Set(n), variableToCollectName, collectionName)

    val rewritten = rewrite(pc)

    rewritten should equal(
      ListIRExpression(
        queryWith(
          QueryGraph(
            patternNodes = Set(n.name, m.name, o.name),
            argumentIds = Set(n.name),
            patternRelationships = Set(
              PatternRelationship(
                r.name,
                (n.name, m.name),
                OUTGOING,
                Seq(relTypeName("R"), relTypeName("P")),
                SimplePatternLength
              ),
              PatternRelationship(r2.name, (m.name, o.name), INCOMING, Seq.empty, SimplePatternLength)
            ),
            selections = Selections.from(Seq(
              not(equals(r, r2)),
              rPred,
              oPred,
              equals(prop(r.name, "prop"), literalInt(5)),
              equals(prop(o.name, "prop"), literalInt(5)),
              not(hasALabel(o.name))
            ))
          ),
          Some(RegularQueryProjection(Map(variableToCollectName -> literalInt(5))))
        ),
        variableToCollectName,
        collectionName,
        s"[(${n.name})-[${r.name}:R|P {prop: 5} WHERE ${r.name}.foo > 5]->(${m.name})<-[${r2.name}]-(${o.name}:!% {prop: 5} WHERE ${o.name}.foo > 5) | 5]"
      )(pos)
    )
  }

}
