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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption.ir
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption.irFromConfig
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption.lp
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidCypherOption

class DeriveEagerAnalyzerOptionTest extends CypherFunSuite with AstConstructionTestSupport {

  private val unsupportedOperationsForIR: Seq[ASTNode] = Seq(
    setLabelItem(node = "foo", labels = Seq("bar"), dynamicLabels = Seq(varFor("A"))),
    setLabelItem(node = "foo", labels = Seq.empty, dynamicLabels = Seq(varFor("A")), containsIs = true),
    SetClause(Seq(setLabelItem(node = "foo", labels = Seq.empty, dynamicLabels = Seq(varFor("A")))))(pos),
    SetDynamicPropertyItem(containerIndex(varFor("A"), 0), literalString("bar"))(pos),
    SetClause(Seq(SetDynamicPropertyItem(containerIndex(varFor("A"), 0), literalString("bar"))(pos)))(pos),
    removeLabelItem(node = "foo", labels = Seq("bar"), dynamicLabels = Seq(varFor("A"))),
    removeLabelItem(node = "foo", labels = Seq.empty, dynamicLabels = Seq(varFor("A")), containsIs = true),
    Remove(Seq(removeLabelItem(node = "foo", labels = Seq.empty, dynamicLabels = Seq(varFor("A")))))(pos),
    RemoveDynamicPropertyItem(containerIndex(varFor("A"), 0)),
    Remove(Seq(RemoveDynamicPropertyItem(containerIndex(varFor("A"), 0))))(pos)
  )

  private val supportedOperationsForIR: Seq[ASTNode] = Seq(
    setLabelItem(node = "foo", labels = Seq("bar")),
    setPropertyItem("r", "prop", literalInt(1)),
    SetClause(Seq(
      setLabelItem(node = "foo", labels = Seq("bar")),
      setLabelItem(node = "foo", labels = Seq("bar"), containsIs = true),
      setPropertyItem("r", "prop", literalInt(1))
    ))(pos),
    removeLabelItem(node = "foo", labels = Seq("bar")),
    Remove(Seq(
      removeLabelItem(node = "foo", labels = Seq("bar")),
      removePropertyItem("foo", "bar"),
      removeLabelItem("foo", Seq("bar"), containsIs = true)
    ))(pos),
    removePropertyItem("foo", "bar"),
    varFor("a"),
    prop("b", "foo")
  )

  def testLogicalPlanState: LogicalPlanState = new LogicalPlanState(
    queryText = "",
    plannerName = IDPPlannerName,
    planningAttributes = PlanningAttributes.newAttributes,
    anonymousVariableNameGenerator = new AnonymousVariableNameGenerator
  )

  def semanticTable(exprs: ASTNode*): SemanticTable = {
    val types = exprs.map(expr => expr -> Scope.empty)
    SemanticTable(recordedScopes = ASTAnnotationMap[ASTNode, Scope](types: _*))
  }

  def mockPlannerContext(eagerAnalyzer: CypherEagerAnalyzerOption): PlannerContext = {
    val plannerContext = mock[PlannerContext]
    when(plannerContext.tracer).thenReturn(NO_TRACING)
    when(plannerContext.cancellationChecker).thenReturn(CancellationChecker.NeverCancelled)
    when(plannerContext.eagerAnalyzer).thenReturn(eagerAnalyzer)
    plannerContext
  }

  supportedOperationsForIR.foreach(supportedOp =>
    test(s"should not translate ir for queries that support ir eagerness like $supportedOp") {
      DeriveEagerAnalyzerOption.transform(
        testLogicalPlanState.withSemanticTable(semanticTable(
          supportedOp
        )),
        mockPlannerContext(ir)
      ).maybeEagerAnalyzerOption shouldBe Some(ir)
    }
  )

  supportedOperationsForIR.foreach(supportedOp =>
    test(s"should translate ir_from_config to ir for queries that support ir eagerness like $supportedOp") {
      DeriveEagerAnalyzerOption.transform(
        testLogicalPlanState.withSemanticTable(semanticTable(
          supportedOp
        )),
        mockPlannerContext(irFromConfig)
      ).maybeEagerAnalyzerOption shouldBe Some(ir)
    }
  )

  (supportedOperationsForIR ++ unsupportedOperationsForIR).foreach(op =>
    test(s"should not translate lp for $op") {
      DeriveEagerAnalyzerOption.transform(
        testLogicalPlanState.withSemanticTable(semanticTable(
          op
        )),
        mockPlannerContext(lp)
      ).maybeEagerAnalyzerOption shouldBe Some(lp)
    }
  )

  unsupportedOperationsForIR.foreach(unsupportedIREagerExpr =>
    test(s"should translate ir_from_config to lp for $unsupportedIREagerExpr") {
      DeriveEagerAnalyzerOption.transform(
        testLogicalPlanState.withSemanticTable(semanticTable(
          unsupportedIREagerExpr
        )),
        mockPlannerContext(irFromConfig)
      ).maybeEagerAnalyzerOption shouldBe Some(lp)
    }
  )

  unsupportedOperationsForIR.foreach(unsupportedIREagerExpr =>
    test(s"should throw if eagerAnalyzer option is ir for $unsupportedIREagerExpr") {
      val invalidCypherOption = the[InvalidCypherOption] thrownBy DeriveEagerAnalyzerOption.transform(
        testLogicalPlanState.withSemanticTable(semanticTable(
          unsupportedIREagerExpr
        )),
        mockPlannerContext(ir)
      )
      invalidCypherOption.getMessage should startWith("The Cypher option `eagerAnalyzer=ir` is not supported")
    }
  )
}
