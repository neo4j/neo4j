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
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.scoping.CommonContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.OmittedResult
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionItem
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionPart
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Result
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResultWithNotYetKnownColumns
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.Acc
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.Aggregating
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.DeclaringContext
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ForeachContext
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.InForeach
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.MatchingPattern
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.NextStatement
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.NonAggregating
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.NotInForeach
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.Opinionated
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ProjectionContext
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ReturnContext
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubqueryExpression
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.Unopinionated
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.VariableContext
import org.neo4j.cypher.internal.util.CallableName
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.gqlstatus.ErrorGqlStatusObject

object ViewModel {

  sealed trait ContentViewModel

  case class ScalarContentViewModel(
    label: String,
    value: String,
    preserveWhitespace: Boolean = false,
    italic: Boolean = false
  ) extends ContentViewModel
  case class VariableListContentViewModel(label: String, values: Seq[LogicalVariable]) extends ContentViewModel

  /** A list of reference -> declaration connections, e.g. for the `referenced` channels. */
  case class ReferenceListContentViewModel(label: String, values: Seq[(LogicalVariable, LogicalVariable)])
      extends ContentViewModel

  case class CallableSignatureListContentViewModel(label: String, values: Seq[LocalCallableScopeSignature])
      extends ContentViewModel
  case class CallableNameListContentViewModel(values: Seq[CallableName]) extends ContentViewModel
  case class ProjectionItemListContentViewModel(label: String, values: Seq[ProjectionItem]) extends ContentViewModel

  case class ErrorListContentViewModel(values: Seq[(ErrorGqlStatusObject, Int)])
      extends ContentViewModel
  case class NestedCardContentViewModel(card: CardViewModel) extends ContentViewModel

  case class CardViewModel(title: String, contents: Seq[ContentViewModel])

  case class ScopeNodeViewModel(
    id: String,
    scopeKind: String,
    astPreview: String,
    details: Seq[CardViewModel],
    variableCheckerEntries: Seq[CardViewModel],
    children: Seq[ScopeNodeViewModel]
  )

  case class InspectionViewModel(warnings: Seq[String], root: ScopeNodeViewModel)

  object InspectionViewModelBuilder {

    def build(
      rootScope: WorkingScope,
      warnings: Seq[String],
      logs: Seq[(Ref[WorkingScope], Acc, VariableCheckerInspectionPoint)]
    ): InspectionViewModel = {
      val logsByScope =
        logs.zipWithIndex.foldLeft(Map.empty[Ref[WorkingScope], Vector[(Acc, VariableCheckerInspectionPoint, Int)]]) {
          case (acc, ((scopeRef, logAcc, inspectionPoint), index)) =>
            acc.updated(scopeRef, acc.getOrElse(scopeRef, Vector.empty) :+ ((logAcc, inspectionPoint, index)))
        }
      InspectionViewModel(warnings, buildScopeNode(rootScope, "scope-0", logsByScope))
    }

    private def buildScopeNode(
      scope: WorkingScope,
      id: String,
      logsByScope: Map[Ref[WorkingScope], Vector[(Acc, VariableCheckerInspectionPoint, Int)]]
    ): ScopeNodeViewModel = {
      val exactScopeLogs = logsByScope.getOrElse(Ref(scope), Vector.empty).map { case (acc, inspectionPoint, index) =>
        buildVariableCheckerEntryCard(acc, inspectionPoint, index)
      }
      ScopeNodeViewModel(
        id = id,
        scopeKind = Formatting.camelCaseToLowerCaseWithSpaces(scope.getClass.getSimpleName),
        astPreview = Formatting.prettify(scope.astNode),
        details = Seq(
          buildContextCard(extractIncoming(scope)),
          buildReferencedCard(scope.referenced),
          buildDeclarationsCard("declared", scope.declared),
          buildRegularContextCard("outgoing", scope.outgoing),
          buildResultCard(scope.result)
        ),
        variableCheckerEntries = exactScopeLogs,
        children = scope.children.zipWithIndex.map { case (child, idx) =>
          buildScopeNode(child, s"$id-$idx", logsByScope)
        }
      )
    }

    private def extractIncoming(scope: WorkingScope): WorkingContext = scope match {
      case PatternScope(_, patternIncoming, _, _, _, _) => patternIncoming
      case other                                        => other.incoming
    }

    private def buildContextCard(incoming: WorkingContext): CardViewModel = incoming match {
      case patternIncomingContext: PatternIncomingContext =>
        CardViewModel(
          title = "Pattern incoming",
          contents = Seq(
            VariableListContentViewModel("topological constants", patternIncomingContext.topologicalConstants.toSeq),
            VariableListContentViewModel("predicate constants", patternIncomingContext.predicateConstants.toSeq),
            VariableListContentViewModel("path constants", patternIncomingContext.pathConstants.toSeq),
            VariableListContentViewModel("group constants", patternIncomingContext.groupConstants.toSeq),
            CallableSignatureListContentViewModel("local callables", patternIncomingContext.localCallables.toSeq)
          )
        )
      case regularContext: RegularContext =>
        buildRegularContextCard("Regular incoming", regularContext)
    }

    private def buildRegularContextCard(labelText: String, context: RegularContext): CardViewModel = {
      val normalized = context match {
        case cc: CommonContext               => cc
        case pc: ProjectionExpressionContext => CommonContext(pc.constants, pc.variables, pc.localCallables)
      }
      val title = context match {
        case _: ProjectionExpressionContext => "Projection expression incoming"
        case _                              => labelText
      }
      val projectionContents = context match {
        case pc: ProjectionExpressionContext =>
          Seq(
            NestedCardContentViewModel(buildProjectionPartCard(pc.projectionPart)),
            NestedCardContentViewModel(buildProjectionSpecificationCard(pc.projectionSpecification))
          )
        case _ => Seq.empty
      }
      CardViewModel(
        title = title,
        contents = Seq(
          VariableListContentViewModel("constants", normalized.constants.toSeq),
          VariableListContentViewModel("variables", normalized.variables.toSeq),
          CallableSignatureListContentViewModel("local callables", normalized.localCallables.toSeq)
        ) ++ projectionContents
      )
    }

    private def buildReferencedCard(references: References): CardViewModel = {
      def pairs(channel: Map[Ref[LogicalVariable], Ref[LogicalVariable]]): Seq[(LogicalVariable, LogicalVariable)] =
        channel.iterator.map { case (reference, declaration) => (reference.value, declaration.value) }.distinct.toSeq
      val hiddenContents =
        if (references.hidden.nonEmpty) Seq(ReferenceListContentViewModel("hidden", pairs(references.hidden)))
        else Seq.empty
      CardViewModel(
        title = "referenced",
        contents = ReferenceListContentViewModel("references", pairs(references.references)) +: hiddenContents
      )
    }

    private def buildDeclarationsCard(labelText: String, declarations: Declarations): CardViewModel =
      CardViewModel(
        title = labelText,
        contents = Seq(
          VariableListContentViewModel("constants", declarations.constants.toSeq),
          VariableListContentViewModel("variables", declarations.variables.toSeq),
          CallableSignatureListContentViewModel("local callables", declarations.localCallables.toSeq)
        )
      )

    private def buildResultCard(result: Result): CardViewModel = {
      val contents = result match {
        case TableResult(columns) =>
          Seq(
            ScalarContentViewModel("type", Formatting.getClassNameWithoutTrailingDollarSign(result)),
            VariableListContentViewModel("columns", columns.toSeq)
          )
        case TableResultWithNotYetKnownColumns | OmittedResult | NoResult | ExpressionResult =>
          Seq(ScalarContentViewModel("type", Formatting.getClassNameWithoutTrailingDollarSign(result)))
      }
      CardViewModel("result", contents)
    }

    private def buildProjectionPartCard(projectionPart: ProjectionPart): CardViewModel =
      CardViewModel(
        title = "Projection part",
        contents = Seq(
          ScalarContentViewModel("type", Formatting.getClassNameWithoutTrailingDollarSign(projectionPart)),
          ScalarContentViewModel("is subclause", projectionPart.isSubclause.toString)
        )
      )

    private def buildProjectionSpecificationCard(projectionSpecification: ProjectionSpecification): CardViewModel =
      CardViewModel(
        title = "Projection specification",
        contents = Seq(
          ProjectionItemListContentViewModel("grouping keys", projectionSpecification.groupingKeys.toSeq),
          ProjectionItemListContentViewModel("nonAggregating items", projectionSpecification.nonAggregatingItems.toSeq),
          ProjectionItemListContentViewModel("aggregating items", projectionSpecification.aggregatingItems.toSeq),
          ScalarContentViewModel("distinct", projectionSpecification.distinct.toString),
          ScalarContentViewModel("has group by", projectionSpecification.hasGroupBy.toString)
        )
      )

    private def buildVariableCheckerEntryCard(
      acc: Acc,
      inspectionPoint: VariableCheckerInspectionPoint,
      index: Int
    ): CardViewModel = {
      val inspectionPointLabel =
        Formatting.camelCaseToLowerCaseWithSpaces(Formatting.getClassNameWithoutTrailingDollarSign(inspectionPoint))
      CardViewModel(
        title = s"$inspectionPointLabel (#$index)",
        contents = Seq(
          NestedCardContentViewModel(buildScopeContextCard(acc.scopeContext)),
          NestedCardContentViewModel(buildVariableContextCard(acc.variableContext)),
          NestedCardContentViewModel(buildProjectionContextCard(acc.projectionContext)),
          NestedCardContentViewModel(buildForeachContextCard(acc.foreachContext))
        ) ++ (
          if (acc.definedLocalCallableNames.nonEmpty)
            Seq(NestedCardContentViewModel(buildDefinedLocalCallableNamesCard(acc.definedLocalCallableNames.toSeq)))
          else Seq.empty
        ) ++ buildErrorsCardContent(acc.errors.toSeq).toSeq
      )
    }

    private def buildScopeContextCard(scopeContext: ReturnContext): CardViewModel = scopeContext match {
      case Unopinionated =>
        CardViewModel("Unopinionated scope context", Seq())
      case o: Opinionated =>
        val title = o match {
          case _: SubqueryExpression => "Subquery expression scope context"
          case _: NextStatement      => "Next statement scope context"
        }
        CardViewModel(title, Seq(VariableListContentViewModel("constants", o.constants.toSeq)))
    }

    private def buildVariableContextCard(variableContext: VariableContext): CardViewModel = variableContext match {
      case context: DeclaringContext =>
        val title = if (context.isInstanceOf[MatchingPattern]) {
          "Matching pattern variable context"
        } else {
          "Updating pattern variable context"
        }
        CardViewModel(
          title,
          Seq(
            VariableListContentViewModel("declared", context.declared.toSeq),
            VariableListContentViewModel("pattern variables", context.patternVariables.toSeq),
            ScalarContentViewModel("ast", Formatting.prettify(context.ast), preserveWhitespace = true),
            ScalarContentViewModel("in relationship", context.inRelationship.toString)
          )
        )
      case other =>
        CardViewModel(
          s"${Formatting.camelCaseToLowerCaseWithSpaces(Formatting.getClassNameWithoutTrailingDollarSign(other))} Variable context",
          Seq.empty
        )
    }

    private def buildProjectionContextCard(projectionContext: ProjectionContext): CardViewModel =
      projectionContext match {
        case Aggregating(incomingToClause) =>
          CardViewModel(
            "Aggregating projection context",
            Seq(VariableListContentViewModel("incoming to clause", incomingToClause.toSeq))
          )
        case NonAggregating =>
          CardViewModel("Non aggregating projection context", Seq())
      }

    private def buildForeachContextCard(foreachContext: ForeachContext): CardViewModel = foreachContext match {
      case InForeach(allowedToShadow) =>
        CardViewModel(
          "In FOREACH context",
          Seq(VariableListContentViewModel("Allowed to shadow", allowedToShadow.toSeq))
        )
      case NotInForeach =>
        CardViewModel("Not in FOREACH context", Seq())
    }

    private def buildDefinedLocalCallableNamesCard(callableNames: Seq[CallableName]): CardViewModel =
      CardViewModel(
        "defined local callable names",
        Seq(CallableNameListContentViewModel(callableNames))
      )

    private def buildErrorsCardContent(errors: Seq[SemanticError]): Option[ContentViewModel] =
      if (errors.nonEmpty)
        Some(
          NestedCardContentViewModel(
            CardViewModel(
              "Semantic errors",
              Seq(
                ErrorListContentViewModel(
                  errors.map(error =>
                    error.gqlStatusObject -> error.position.offset
                  )
                )
              )
            )
          )
        )
      else None
  }
}
