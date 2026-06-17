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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.scoping.AprioriScope
import org.neo4j.cypher.internal.ast.semantics.scoping.CommonContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.OmittedResult
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionItem
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Result
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResultWithNotYetKnownColumns
import org.neo4j.cypher.internal.ast.semantics.scoping.UnexpectedAstNodeScopingError
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.plandescription.Arguments.Comment
import org.neo4j.cypher.internal.plandescription.Arguments.DeclaredCallables
import org.neo4j.cypher.internal.plandescription.Arguments.DeclaredConstants
import org.neo4j.cypher.internal.plandescription.Arguments.DeclaredVariables
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingCallables
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingConstants
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingPath
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingPredicate
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingProjectionItems
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingTopology
import org.neo4j.cypher.internal.plandescription.Arguments.IncomingVariables
import org.neo4j.cypher.internal.plandescription.Arguments.OutgoingCallables
import org.neo4j.cypher.internal.plandescription.Arguments.OutgoingConstants
import org.neo4j.cypher.internal.plandescription.Arguments.OutgoingVariables
import org.neo4j.cypher.internal.plandescription.Arguments.Referenced
import org.neo4j.cypher.internal.plandescription.Arguments.ResultColumns
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id

object WorkingScope2PlanDescription {

  def apply(workingScope: WorkingScope): InternalPlanDescription = {
    create(workingScope, inNum = 1)
  }

  private def create(workingScope: WorkingScope, inNum: Int): InternalPlanDescription = {
    val (_, children) = workingScope.children.foldLeft((inNum, Seq.empty[InternalPlanDescription])) {
      case ((prevInNum, prevChildren), thisWorkingScope) =>
        val thisInNum = prevInNum + 1
        (thisInNum, prevChildren :+ create(thisWorkingScope, thisInNum))
    }

    workingScope match {
      case StatementScope(astNode, incoming, referenced, declared, outgoing, result, _, _) =>
        WorkingScopePlanDescription(
          id = Id(inNum),
          name = renderAstString(astNode),
          variables = renderVariables(incoming),
          arguments = Seq(
            renderIncoming(incoming),
            renderReferenced(referenced.getVariables.toSet),
            renderDeclaration(declared),
            renderResult(result),
            renderOutgoing(result, outgoing)
          ).flatten,
          children = children
        )
      case ExpressionScope(astNode, incoming, referenced, declared, _) =>
        WorkingScopePlanDescription(
          id = Id(inNum),
          name = renderAstString(astNode),
          variables = renderVariables(incoming),
          arguments = Seq(
            renderIncoming(incoming),
            renderReferenced(referenced.getVariables.toSet),
            renderDeclaration(declared)
          ).flatten,
          children = children
        )
      case PatternScope(astNode, patternIncoming, referenced, declared, result, _) =>
        WorkingScopePlanDescription(
          id = Id(inNum),
          name = renderAstString(astNode),
          variables = renderVariables(patternIncoming),
          arguments = Seq(
            renderIncoming(patternIncoming),
            renderReferenced(referenced.getVariables.toSet),
            renderDeclaration(declared),
            renderResult(result)
          ).flatten,
          children = children
        )
      case AprioriScope(_, outgoing) =>
        WorkingScopePlanDescription(
          id = Id(inNum),
          name = "apriori",
          variables = Set.empty,
          arguments = Seq(
            renderOutgoingWorkingContext(outgoing)
          ).flatten,
          children = children
        )
      case UnexpectedAstNodeScopingError(astNode, incoming) =>
        WorkingScopePlanDescription(
          id = Id(inNum),
          name = renderAstString(astNode),
          variables = renderVariables(incoming),
          arguments = Seq(
            renderIncoming(incoming),
            Seq(Comment("unexpected syntax"))
          ).flatten,
          children = children
        )
    }
  }

  private val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  private def renderAstString(astNode: ASTNode): String = astNode match {
    case s: Statement           => prettifier.asString(s)
    case c: Clause              => prettifier.asString(SingleQuery(Seq(c))(InputPosition.NONE))
    case gb: GroupBy            => prettifier.asString(gb)
    case ex: Expression         => prettifier.expr(ex)
    case p: Pattern             => prettifier.expr.patterns(p)
    case p: PatternPart         => prettifier.expr.patterns(p)
    case p: PatternElement      => prettifier.expr.patterns(p)
    case p: RelationshipPattern => prettifier.expr.patterns(p)
    case lex: LabelExpression   => prettifier.expr.stringifyLabelExpression(lex)
    case cqb @ ConditionalQueryBranch(Some(_), _) =>
      prettifier.asString(ConditionalQueryWhen(Seq(cqb), None)(InputPosition.NONE))
    case cqb @ ConditionalQueryBranch(None, _) =>
      prettifier.asString(ConditionalQueryWhen(Seq(), Some(cqb))(InputPosition.NONE))
    case x => x.toString
  }

  private def renderVariables(incoming: WorkingContext): Set[PrettyString] = {
    incoming.allSymbols.map(v => PrettyString(v.name))
  }

  private def renderIncoming(incoming: WorkingContext): Seq[Argument] = {
    incoming match {
      case CommonContext(constants, variables, localCallables) => Seq(
          IncomingConstants(renderVariableSet(constants)),
          IncomingVariables(renderVariableSet(variables)),
          IncomingCallables(renderCallableSet(localCallables))
        )
      case PatternIncomingContext(topology, predicate, path, _, localCallables) => Seq(
          IncomingTopology(renderVariableSet(topology)),
          IncomingPredicate(renderVariableSet(predicate)),
          IncomingPath(renderVariableSet(path)),
          IncomingCallables(renderCallableSet(localCallables))
        )
      case ProjectionExpressionContext(constants, variables, localCallables, projectionSpecification, _) => Seq(
          IncomingConstants(renderVariableSet(constants)),
          IncomingVariables(renderVariableSet(variables)),
          IncomingCallables(renderCallableSet(localCallables)),
          IncomingProjectionItems(renderProjectionSpecification(projectionSpecification))
        )
    }
  }

  private def renderDeclaration(declarations: Declarations): Seq[Argument] = {
    declarations match {
      case Declarations(constants, variables, localCallables)
        if constants.isEmpty && variables.isEmpty && localCallables.isEmpty => Seq.empty
      case Declarations(constants, variables, localCallables) =>
        Seq(
          DeclaredConstants(renderVariableSeq(constants)),
          DeclaredVariables(renderVariableSeq(variables)),
          DeclaredCallables(renderCallableSeq(localCallables))
        )
    }
  }

  private def renderReferenced(referenced: Set[LogicalVariable]): Seq[Argument] = {
    Seq(Referenced(renderVariableSet(referenced)))
  }

  private def renderResult(result: Result): Seq[Argument] = {
    result match {
      case TableResult(columns)              => Seq(ResultColumns(renderVariableSeq(columns)))
      case TableResultWithNotYetKnownColumns => Seq(ResultColumns("- Not yet known columns -"))
      case OmittedResult                     => Seq(ResultColumns("- Omitted -"))
      case NoResult                          => Seq.empty
      case ExpressionResult                  => Seq.empty
    }
  }

  private def renderOutgoing(result: Result, outgoing: WorkingContext): Seq[Argument] = {
    result match {
      case TableResult(_)                    => Seq.empty
      case TableResultWithNotYetKnownColumns => Seq.empty
      case OmittedResult                     => renderOutgoingWorkingContext(outgoing)
      case NoResult                          => renderOutgoingWorkingContext(outgoing)
      case ExpressionResult                  => Seq.empty
    }
  }

  private def renderOutgoingWorkingContext(outgoing: WorkingContext): Seq[Argument] =
    outgoing match {
      case rc: RegularContext => Seq(
          OutgoingConstants(renderVariableSet(rc.constants)),
          OutgoingVariables(renderVariableSet(rc.variables)),
          OutgoingCallables(renderCallableSet(rc.localCallables))
        )
      case _ => Seq.empty
    }

  private def renderVariableSet(variables: Set[LogicalVariable]): String = {
    variables.toSeq.sortBy(_.position.offset).map(renderVariable).mkString(", ")
  }

  private def renderCallableSet(callables: Set[LocalCallableScopeSignature]): String = {
    callables.toSeq.sortBy(_.name.fullName).map(renderCallable).mkString(", ")
  }

  private def renderProjectionSpecification(spec: ProjectionSpecification): String = {
    if (spec.isEmpty) {
      "—"
    } else {
      val stringifier = ExpressionStringifier()
      spec.items.map {
        case ProjectionItem(expr, alias) => stringifier(expr) + alias.map(a => s" -> ${a.name}").getOrElse("")
        case _                           => "—"
      }
    }.mkString(", ")
  }

  private def renderVariableSeq(variables: Seq[LogicalVariable]): String = {
    variables.map(renderVariable).mkString(", ")
  }

  private def renderCallableSeq(callables: Seq[LocalCallableScopeSignature]): String = {
    callables.map(renderCallable).mkString(", ")
  }

  private def renderVariable(variable: LogicalVariable): String =
    variable.name

  private def renderCallable(callable: LocalCallableScopeSignature): String =
    callable.name.fullName
}
