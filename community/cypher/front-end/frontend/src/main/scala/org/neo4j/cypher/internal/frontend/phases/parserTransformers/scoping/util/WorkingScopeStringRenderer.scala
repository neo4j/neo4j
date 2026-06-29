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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.scoping.AprioriScope
import org.neo4j.cypher.internal.ast.semantics.scoping.CommonContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.GroupingKey
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.OmittedResult
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.References
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
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.Block
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.BoxedBlock
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.BoxedBlockPositionInSequence
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.EpsilonBlock
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.EpsilonSpan
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.Span
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.util.Text
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

object WorkingScopeStringRenderer {

  def apply(workingScopeOpt: Option[WorkingScope]): String = {
    val renderedText = workingScopeOpt.map(render).getOrElse(Span("— no scope found —"))
    val renderedString = renderedText.render()
    renderedString
  }

  def apply(workingScope: WorkingScope): String = render(workingScope).render()

  private def render(workingScope: WorkingScope): Text = workingScope match {
    case StatementScope(astNode, incoming, referenced, declared, outgoing, result, children, _) =>
      Block(
        renderIncoming(incoming),
        renderAst(astNode),
        renderReferenced(referenced),
        renderHidden(referenced),
        renderDeclaration(declared),
        renderChildren(children),
        renderResult(result),
        renderOutgoing(result, outgoing)
      )
    case ExpressionScope(astNode, patternIncoming, referenced, declared, children) =>
      Block(
        renderIncoming(patternIncoming),
        renderAst(astNode),
        renderReferenced(referenced),
        renderHidden(referenced),
        renderDeclaration(declared),
        renderChildren(children)
      )
    case PatternScope(astNode, patternIncoming, referenced, declared, result, children) =>
      Block(
        renderIncoming(patternIncoming),
        renderAst(astNode),
        renderReferenced(referenced),
        renderHidden(referenced),
        renderDeclaration(declared),
        renderChildren(children),
        renderResult(result)
      )
    case AprioriScope(_, outgoing) =>
      // this one is not expected to actually show up
      Block(
        Span("— apriori —"),
        Span(s"${renderWorkingContext(outgoing)}")
      )
    case UnexpectedAstNodeScopingError(astNode, incoming) =>
      Block(
        Span(s"${renderWorkingContext(incoming)}"),
        Span(
          Span("— unexpected syntax: "),
          Span.shrinkAtEnd(renderAstString(astNode)),
          Span(" —")
        )
      )
  }

  private def renderIncoming(incoming: WorkingContext): Span = {
    Span(s"In ${renderWorkingContext(incoming)}")
  }

  private def renderAst(astNode: ASTNode): Span = {
    Span.shrinkInMiddle(renderAstString(astNode))
  }

  private def renderDeclaration(declarations: Declarations): Text = {
    declarations match {
      case Declarations(constants, variables, localCallables) if constants.isEmpty && variables.isEmpty => EpsilonSpan
      case Declarations(constants, variables, localCallables) =>
        Span(
          s"Decl Const: ${renderVariableSeq(constants)} | Var: ${renderVariableSeq(variables)} | Call: ${renderCallableSeq(localCallables)}"
        )
    }
  }

  private def renderReferenced(referenced: References): Text = {
    val variables = referenced.getVariables.toSet
    if (variables.isEmpty) {
      EpsilonSpan
    } else {
      Span(s"Ref: ${renderVariableSet(variables)}")
    }
  }

  private def renderHidden(referenced: References): Text = {
    val variables = referenced.hiddenRefs.getVariables.toSet
    if (variables.isEmpty) {
      EpsilonSpan
    } else {
      Span(s"Hidden: ${renderVariableSet(variables)}")
    }
  }

  private def renderChildren(children: Seq[WorkingScope]): Text = children match {
    case Nil        => EpsilonBlock
    case Seq(child) => BoxedBlock(render(child))
    case firstChild +: middleChildren :+ lastChild =>
      Block(
        BoxedBlock(BoxedBlockPositionInSequence.First, render(firstChild)),
        Block(middleChildren.map(m => BoxedBlock(BoxedBlockPositionInSequence.Middle, render(m))): _*),
        BoxedBlock(BoxedBlockPositionInSequence.Last, render(lastChild))
      )
    case _ => EpsilonBlock // to make compiler happy
  }

  private def renderResult(result: Result): Text = {
    result match {
      case TableResult(columns)              => Span(s"Result columns: ${renderVariableSeq(columns)}")
      case TableResultWithNotYetKnownColumns => Span(s"Not yet known result columns")
      case OmittedResult                     => Span(s"Omitted result")
      case NoResult                          => EpsilonSpan
      case ExpressionResult                  => EpsilonSpan
    }
  }

  private def renderOutgoing(result: Result, outgoing: WorkingContext): Text = {
    result match {
      case TableResult(_)                    => EpsilonSpan
      case TableResultWithNotYetKnownColumns => EpsilonSpan
      case OmittedResult                     => Span(s"Out ${renderWorkingContext(outgoing)}")
      case NoResult                          => Span(s"Out ${renderWorkingContext(outgoing)}")
      case ExpressionResult                  => EpsilonSpan
    }
  }

  private def renderWorkingContext(workingContext: WorkingContext): String = workingContext match {
    case CommonContext(constants, variables, localCallables) =>
      s"Const: ${renderVariableSet(constants)}; Var: ${renderVariableSet(variables)}; Call: ${renderCallableSet(localCallables)}"
    case ProjectionExpressionContext(constants, variables, localCallables, spec, _) =>
      s"Const: ${renderVariableSet(constants)}; Var: ${renderVariableSet(variables)}; Call: ${renderCallableSet(localCallables)}; Keys: ${renderProjectionSpecification(spec)}";
    case PatternIncomingContext(
        topologicalConstants,
        predicateConstants,
        pathConstants,
        groupConstants,
        localCallables
      ) =>
      s"Topo: ${renderVariableSet(topologicalConstants)}; Pred: ${renderVariableSet(predicateConstants)}; Path: ${renderVariableSet(pathConstants)}; Group: ${renderVariableSet(groupConstants)}; Call: ${renderCallableSet(localCallables)}"
  }

  private def renderVariableSet(variables: Set[LogicalVariable]): String = {
    renderVariableSeq(variables.toSeq.sortBy(_.position.offset))
  }

  private def renderProjectionSpecification(spec: ProjectionSpecification): String =
    if (spec.groupingKeys.isEmpty) "—"
    else spec.groupingKeys.iterator.map(renderGroupingKey).mkString(", ")

  private def renderGroupingKey(gk: GroupingKey): String = {
    val expr = stringifier(gk.expression)
    gk.alias match {
      case Some(a) if a.name != expr => s"$expr AS ${a.name}"
      case _                         => expr
    }
  }

  private val stringifier: ExpressionStringifier = ExpressionStringifier()

  private def renderVariableSeq(variables: Seq[LogicalVariable]): String = {
    if (variables.isEmpty) {
      "—"
    } else {
      variables.map(renderVariable).mkString(", ")
    }
  }

  private def renderCallableSet(callables: Set[LocalCallableScopeSignature]): String = {
    renderCallableSeq(callables.toSeq.sortBy(_.name.fullName))
  }

  private def renderCallableSeq(callables: Seq[LocalCallableScopeSignature]): String = {
    if (callables.isEmpty) {
      "—"
    } else {
      callables.map(renderCallable).mkString(", ")
    }
  }

  private def renderCallable(callable: LocalCallableScopeSignature): String =
    (callable.name.namespace.parts :+ callable.name.name).mkString(".")

  // TODO: for the general case, we want to improve this method to deal with long variable name and such that require escaping
  private def renderVariable(variable: LogicalVariable): String = variable.name

  private val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  private def renderAstString(astNode: ASTNode): String = whitespaceNormalization(astNode match {
    case s: Statement           => prettifier.asString(s)
    case c: Clause              => prettifier.asString(SingleQuery(Seq(c))(InputPosition.NONE))
    case gb: GroupBy            => prettifier.asString(gb)
    case gr: GraphReference     => prettifier.expr(gr)
    case s: Search              => prettifier.asString(s)
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
  })

  private def whitespaceNormalization(cypher: String): String =
    cypher.trim.replaceAll("\\s+", " ")
}
