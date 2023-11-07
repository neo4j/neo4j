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
import org.neo4j.cypher.internal.ast.Where
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
 */
case object ShortestPathNodeDeduplicator extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {
  type VariableRenamings = Map[Ref[LogicalVariable], LogicalVariable]

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[_ <: BaseContext, _ <: BaseState, BaseState] = this

  override def process(from: BaseState, context: BaseContext): BaseState = {
    def generateRenaming(variable: LogicalVariable): (Ref[LogicalVariable], LogicalVariable) = {
      val newName = Namespacer.genName(from.anonymousVariableNameGenerator, variable.name)
      Ref(variable) -> variable.renameId(newName)
    }

    val renamings = mutable.Map.empty[Ref[LogicalVariable], LogicalVariable]

    val statementWithPredicates = from.statement().endoRewrite(topDown(Rewriter.lift {
      case clause @ Match(_, _, _, _, _) =>
        val currentScope = from.semantics().scope(clause)
        clause.endoRewrite(topDown(Rewriter.lift {
          case p @ PatternPartWithSelector(_: SelectiveSelector, patternPart) =>
            val element = patternPart.element
            val rewrittenElement = element.endoRewrite(topDown(Rewriter.lift {
              case qpp: QuantifiedPath =>
                val variables = qpp.part.element.allVariablesLeftToRight
                val currentRenamings = variables.groupBy(identity).flatMap {
                  case (_, variables) =>
                    variables.tail.map(generateRenaming)
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
            }))
            val variables = rewrittenElement.allVariablesLeftToRight
            val exterior = Set(variables.head, variables.last)
            val interior = variables.tail.init
            val currentRenamings = interior.groupBy(identity).flatMap {
              case (key, variables) if exterior.contains(key) =>
                variables.map(generateRenaming)
              case (_, variables) =>
                val firstVariable = variables.head
                val variableDefinedInThisClause =
                  currentScope.flatMap(_.symbol(firstVariable.name))
                    .forall(_.definition.asVariable.position == firstVariable.position)
                if (variableDefinedInThisClause) {
                  variables.tail.map(generateRenaming)
                } else {
                  // The variable is defined in previous clause.
                  variables.map(generateRenaming)
                }
            }

            if (currentRenamings.isEmpty && element == rewrittenElement) {
              p
            } else {
              renamings ++= currentRenamings

              val rewriter = bottomUp(Rewriter.lift {
                case v: LogicalVariable => currentRenamings.getOrElse(Ref(v), v)
              })

              val newP = p.replaceElement(rewrittenElement).endoRewrite(rewriter)

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
        }))
    }))

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

  private def equijoinsForRenamings(currentRenamings: Map[Ref[LogicalVariable], LogicalVariable]): ListSet[Expression] =
    currentRenamings.view.map {
      case (from, to) => Equals(to, from.value.copyId)(from.value.position)
    }.to(ListSet)

  override def preConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable + normalizePredicates.completed

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable // Introduces new AST nodes

  override def phase: CompilationPhase = CompilationPhase.AST_REWRITE
}
