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

import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SymbolUse
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.NamedVariable
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable

/**
 * Rename variables so they are all unique.
 */
case object Namespacer extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {
  type VariableRenamings = Map[Ref[LogicalVariable], LogicalVariable]

  override def phase: CompilationPhase = AST_REWRITE

  override def process(from: BaseState, ignored: BaseContext): BaseState = {
    val withProjectedUnions = from.statement().endoRewrite(projectUnions)
    val table = from.semanticTable()

    val ambiguousNames = shadowedNames(from.semantics().scopeTree)

    val variableDefinitions: Map[SymbolUse, SymbolUse] = from.semantics().scopeTree.allVariableDefinitions
    val renamings =
      variableRenamings(withProjectedUnions, variableDefinitions, ambiguousNames, from.anonymousVariableNameGenerator)

    if (renamings.isEmpty) {
      from.withStatement(withProjectedUnions).withSemanticTable(table)
    } else {
      val rewriter = renamingRewriter(renamings)
      val newStatement = withProjectedUnions.endoRewrite(rewriter)
      val newSemanticTable = table.replaceExpressions(rewriter)
      from.withStatement(newStatement).withSemanticTable(newSemanticTable)
    }
  }

  private def shadowedNames(scopeTree: Scope): Set[String] = {
    val definitions = scopeTree.allSymbolDefinitions

    definitions.collect {
      case (name, symbolDefinitions) if symbolDefinitions.size > 1 => name
    }.toSet
  }

  private def variableRenamings(
    statement: Statement,
    variableDefinitions: Map[SymbolUse, SymbolUse],
    ambiguousNames: Set[String],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): VariableRenamings = {
    val newNames = mutable.Map[SymbolUse, String]()

    def createVariableRenaming(
      variable: LogicalVariable,
      anonymousVariableNameGenerator: AnonymousVariableNameGenerator
    ): (Ref[LogicalVariable], LogicalVariable) = {

      val symbolDefinition = variableDefinitions(SymbolUse(variable))
      val name = newNames.getOrElseUpdate(symbolDefinition, genName(anonymousVariableNameGenerator, variable.name))
      val newVariable = variable.renameId(name)
      Ref(variable) -> newVariable
    }

    statement.folder.treeFold(Map.empty[Ref[LogicalVariable], LogicalVariable]) {
      case i: LogicalVariable if ambiguousNames(i.name) =>
        val renaming = createVariableRenaming(i, anonymousVariableNameGenerator)
        acc => TraverseChildren(acc + renaming)
      case e: ExpressionWithComputedDependencies =>
        val introducedVariablesRenamings = e.introducedVariables
          .filter(v => ambiguousNames(v.name))
          .foldLeft(Set[(Ref[LogicalVariable], LogicalVariable)]()) { (innerAcc, v) =>
            innerAcc + createVariableRenaming(v, anonymousVariableNameGenerator)
          }
        val scopeDependenciesRenamings = e.scopeDependencies
          .filter(v => ambiguousNames(v.name))
          .foldLeft(Set[(Ref[LogicalVariable], LogicalVariable)]()) { (innerAcc, v) =>
            innerAcc + createVariableRenaming(v, anonymousVariableNameGenerator)
          }
        acc => TraverseChildren(acc ++ introducedVariablesRenamings ++ scopeDependenciesRenamings)
    }
  }

  /**
   * Generate a unique anonymous name.
   *
   * If the original variable is anonymous, simply create a new anonymous variable.
   * If the original variable is not anonymous,
   * include the original variable name for easier debugging and better plan descriptions.
   */
  def genName(anonymousVariableNameGenerator: AnonymousVariableNameGenerator, variableName: String): String = {
    val nextName = anonymousVariableNameGenerator.nextName
    variableName match {
      case NamedVariable(name) =>
        nextName.replace(AnonymousVariableNameGenerator.generatorName, name + "@")
      case _ =>
        nextName
    }
  }

  def projectUnions: Rewriter = {
    // This needs to be topDown so that Unions do net get copied before being replaced by a ProjectingUnion,
    // otherwise we create new copies of the unionMapping variables which are then unknown to the semantic state.
    topDown(Rewriter.lift {
      case u: UnionAll      => ProjectingUnionAll(u.lhs, u.rhs, u.unionMappings)(u.position)
      case u: UnionDistinct => ProjectingUnionDistinct(u.lhs, u.rhs, u.unionMappings)(u.position)
    })
  }

  private def renamingRewriter(renamings: VariableRenamings): Rewriter = inSequence(
    bottomUp(Rewriter.lift {
      case item @ ProcedureResultItem(None, v: Variable) if renamings.contains(Ref(v)) =>
        item.copy(output = Some(ProcedureOutput(v.name)(v.position)))(item.position)
    }),
    bottomUp(Rewriter.lift {
      case v: Variable =>
        renamings.get(Ref(v)) match {
          case Some(newVariable) => newVariable
          case None              => v
        }
      case e: ExpressionWithComputedDependencies =>
        val newIntroducedVariables = e.introducedVariables.map(v => {
          renamings.get(Ref(v)) match {
            case Some(newVariable) => newVariable
            case None              => v
          }
        })
        val newScopeDependencies = e.scopeDependencies.map(v => {
          renamings.get(Ref(v)) match {
            case Some(newVariable) => newVariable
            case None              => v
          }
        })
        e.withComputedIntroducedVariables(newIntroducedVariables).withComputedScopeDependencies(newScopeDependencies)
    })
  )

  override def preConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def postConditions: Set[StepSequencer.Condition] = Set(
    StatementCondition(containsNoNodesOfType[UnionAll]()),
    StatementCondition(containsNoNodesOfType[UnionDistinct]()),
    completed
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable // Introduces new AST nodes

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[BaseContext, BaseState, BaseState] = this
}
