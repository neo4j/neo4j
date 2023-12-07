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

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.normalizePredicates
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.topDown

import scala.collection.immutable.ListSet
import scala.collection.mutable

/**
 * Rename interior variables in a shortest path pattern.
 * TODO add tests for relationships
 */
case object ShortestPathVariableDeduplicator extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[_ <: BaseContext, _ <: BaseState, BaseState] = this

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val renamings = mutable.Map.empty[Ref[LogicalVariable], LogicalVariable]

    val statementWithPredicates = from.statement().endoRewrite(
      topDown(Rewriter.lift {
        case clause: Match =>
          val currentScope = from.semantics().recordedScopes(clause).scope
          clause.endoRewrite(
            clauseRewriter(from.anonymousVariableNameGenerator, currentScope, renamings)
          )
      })
    )

    if (renamings.isEmpty) {
      from
    } else {
      // we need to rewrite the pattern
      val newStatement = statementWithPredicates.endoRewrite(bottomUp(Rewriter.lift {
        case v: LogicalVariable => renamings.getOrElse(Ref(v), v)
      }))
      from.withStatement(newStatement)
    }
  }

  private def generateRenaming(anonymousVariableNameGenerator: AnonymousVariableNameGenerator)(
    variable: LogicalVariable
  ): (Ref[LogicalVariable], LogicalVariable) = {
    val newName = Namespacer.genName(anonymousVariableNameGenerator, variable.name)
    Ref(variable) -> variable.renameId(newName)
  }

  private def clauseRewriter(
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    currentScope: Scope,
    renamings: mutable.Map[Ref[LogicalVariable], LogicalVariable]
  ): Rewriter = {
    val innerRewriter = patternElementRewriter(anonymousVariableNameGenerator, renamings)

    topDown(
      Rewriter.lift {
        case p @ PatternPartWithSelector(_: SelectiveSelector, patternPart) =>
          val element = patternPart.element

          val rewrittenElement = element.endoRewrite(innerRewriter)
          val variables = rewrittenElement.allVariablesLeftToRight
          val exterior = Set(variables.head, variables.last)
          val interior = variables.tail.init

          val currentRenamings = interior.groupBy(identity).flatMap {
            case (key, variables) if exterior.contains(key) =>
              variables.map(generateRenaming(anonymousVariableNameGenerator))
            case (_, variables) =>
              val firstVariable = variables.head
              val variableDefinedInThisClause =
                currentScope
                  .symbolTable(firstVariable.name)
                  .definition.asVariable.position == firstVariable.position
              if (variableDefinedInThisClause) {
                variables.tail.map(generateRenaming(anonymousVariableNameGenerator))
              } else {
                // The variable is defined in previous clause.
                variables.map(generateRenaming(anonymousVariableNameGenerator))
              }
          }

          if (currentRenamings.isEmpty && element == rewrittenElement) {
            p
          } else {
            renamings ++= currentRenamings

            val newP = p.replaceElement(rewrittenElement)

            val predicates = equijoinsForRenamings(currentRenamings)
            newP.modifyElement {
              case path: ParenthesizedPath =>
                val where = path.optionalWhereClause
                val newWhere = Where.combineOrCreate(where, predicates)
                path.copy(optionalWhereClause = newWhere)(path.position)
              case otherElement =>
                ParenthesizedPath(PathPatternPart(otherElement), Some(Ands.create(predicates)))(p.position)
            }
          }
      },
      // Don't recurse into subqueries here. The other rewriter will match any MATCH clause
      // and we would otherwise end up twice in here if the MATCH is nested in a subquery in
      // another MATCH.
      stopper = _.isInstanceOf[Query]
    )
  }

  private def patternElementRewriter(
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    renamings: mutable.Map[Ref[LogicalVariable], LogicalVariable]
  ): Rewriter = topDown(Rewriter.lift {
    case qpp: QuantifiedPath =>
      val variables = qpp.part.element.allVariablesLeftToRight
      val currentRenamings = variables.groupBy(identity).flatMap {
        case (_, variables) =>
          variables.tail.map(generateRenaming(anonymousVariableNameGenerator))
      }

      if (currentRenamings.isEmpty) {
        qpp
      } else {
        renamings ++= currentRenamings

        val predicates = equijoinsForRenamings(currentRenamings)

        val newGroupings = currentRenamings.values.map(QuantifiedPath.getGrouping(_, qpp.position))

        val oldWhere = qpp.optionalWhereExpression
        val newWhere = Where.combineOrCreate(oldWhere, predicates)
        qpp.copy(
          optionalWhereExpression = newWhere,
          variableGroupings = qpp.variableGroupings ++ newGroupings
        )(qpp.position)
      }
  })

  private def equijoinsForRenamings(currentRenamings: Map[Ref[LogicalVariable], LogicalVariable]): ListSet[Expression] =
    currentRenamings.view.map {
      case (from, to) => Equals(to, from.value.copyId)(from.value.position)
    }.to(ListSet)

  override def preConditions: Set[StepSequencer.Condition] =
    // Reads scope of MATCH clauses
    SemanticInfoAvailable +
      // Rewrites predicates
      normalizePredicates.completed

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable // Introduces new AST nodes

  override def phase: CompilationPhase = CompilationPhase.AST_REWRITE
}
