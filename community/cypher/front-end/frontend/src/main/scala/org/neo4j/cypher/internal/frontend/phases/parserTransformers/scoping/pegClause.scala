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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ClauseType
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.DefaultYield
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.GroupingAll
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.InputDataStream
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ProjectionClause.Elements
import org.neo4j.cypher.internal.ast.ProjectionClause.Subclauses
import org.neo4j.cypher.internal.ast.ProjectionType
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnType
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SetPropertyItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.WithType
import org.neo4j.cypher.internal.ast.YieldType
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingPart
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.GroupByPart
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalProcedureScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingPart
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingSubclausePart
import org.neo4j.cypher.internal.ast.semantics.scoping.OmittedResult
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Result
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResultWithNotYetKnownColumns
import org.neo4j.cypher.internal.ast.semantics.scoping.UnexpectedAstNodeScopingError
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.unitVariables
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.frontend.phases.ResolvedLocalCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Ref

object pegClause {

  def apply(clause: Clause, incoming: RegularContext, foreachIterVar: Option[LogicalVariable] = None)(implicit
    c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[Clause](
      clause,
      incoming,
      inImportingWith = false,
      foreachIterVar,
      applyUncached(_, _, foreachIterVar)
    )
  }

  private def applyUncached(clause: Clause, incoming: RegularContext, foreachIterVar: Option[LogicalVariable])(implicit
    c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = clause
    clause match {

      /**
       * Clause
       */
      case call @ ImportingWithSubqueryCall(query, inTransactionsParameters, _) =>
        val explicitImportedVariables = query.importColumns.toSet
        val scope = scopeInlineSubquery(
          call,
          incoming,
          explicitImportedVariables,
          innerQueryIncoming = incoming,
          inTransactionsParameters
        )

        val additionalImportingWithRefs = query.getImportingWithItems.flatMap {
          case AliasedReturnItem(v1 @ LogicalVariable(_), v2) =>
            val x = explicitImportedVariables.find(_.name == v1.name)
            x.flatMap(ex => {
              val dec = scope.referenced.getDeclaration(Ref(ex))
              Some(Set(Ref(v1) -> dec, Ref(v2) -> dec))
            })
          case _ => None
        }.flatten

        scope.addReferences(additionalImportingWithRefs)

      case call @ ScopeClauseSubqueryCall(_, isImportingAll, importedVariables, inTransactionsParameters, _, _) =>
        val innerQueryIncoming =
          if (isImportingAll) incoming.constantChildContext()
          else RegularContext(
            constants = incoming.allSymbols.filter(x => importedVariables.exists(x.name == _.name)),
            variables = unitVariables,
            localCallables = incoming.localCallables
          )

        scopeInlineSubquery(
          call,
          incoming,
          importedVariables.toSet,
          innerQueryIncoming,
          inTransactionsParameters
        )

      // unresolved named call
      case UnresolvedCall(procedureName, declaredArguments, declaredResult, isStandalone, _, _) =>
        val children =
          declaredArguments.map(
            _.map(arg => pegExpression(arg, incoming.constantChildContext()))
          ).getOrElse(Seq.empty)
        val referenced = Some(WorkingScope.referencedInChildren(children))

        val locallyResolved = incoming.localCallables.collectFirst {
          case lc: LocalProcedureScopeSignature if lc.name == procedureName => lc
        }

        /**
         *  Local callable:
         *
         *  Definition                   | result                             | outgoing     |
         *  -----------------------------|------------------------------------|--------------|
         *  DEFINE PROCEDURE a(...) {    |                                    |              |
         *    ...                        |                                    |              |
         *    RETURN a, ...              |                                    |              |
         *  }                            |                                    |              |
         *  ...                          |                                    |              |
         *  CALL a(...)                  | NoResult                           | Set("a",...) |
         *  -----------------------------|------------------------------------|--------------|
         *  DEFINE PROCEDURE a(...) {    |                                    |              |
         *    ...                        |                                    |              |
         *    RETURN a, ...              |                                    |              |
         *  }                            |                                    |              |
         *  ...                          |                                    |              |
         *  CALL a(...) YIELD *          | NoResult                           | Set("a",...) |
         *  -----------------------------|------------------------------------|--------------|
         *  DEFINE PROCEDURE ... {       |                                    |              |
         *    ...                        |                                    |              |
         *    RETURN a, ...              |                                    |              |
         *  }                            |                                    |              |
         *  ...                          |                                    |              |
         *  CALL a(...) YIELD a AS x,... | NoResult                           | Set("x",...) |
         *  -----------------------------|------------------------------------|--------------|
         *  DEFINE PROCEDURE ... {       |                                    |              |
         *    ...                        |                                    |              |
         *    FINISH                     |                                    |              |
         *  }                            |                                    |              |
         *  ...                          |                                    |              |
         *  CALL a(...)                  | NoResult                           | Set()        |
         *
         *  Non-local callable:
         *
         *  In standalone context        | result                             | outgoing     |
         *  -----------------------------|------------------------------------|--------------|
         *  CALL db.labels()             | TableResultWithNotYetKnownColumns  | Set()        |
         *  CALL db.labels() YIELD *     | TableResultWithNotYetKnownColumns  | Set()        |
         *  CALL db.labels() YIELD label | TableResult(Seq(var("label")))     | Set("label") |
         *
         *  In inlined context           | result                             | outgoing     |
         *  -----------------------------|------------------------------------|--------------|
         *  CALL db.labels()             | OmittedResult                      | Set()        |
         *  CALL db.labels() YIELD label | NoResult                           | Set("label") |
         *
         */
        locallyResolved match {
          // Local callable
          case Some(LocalProcedureScopeSignature(_, _, _, procedureResult)) =>
            val yieldColumnsOpt = declaredResult.map(_.items.map(_.variable))
            val resultColumns = (procedureResult, yieldColumnsOpt) match {
              // no YIELD and YIELD *
              case (TableResult(columns), None) => columns
              // concrete YIELD a, b, ...
              case (TableResult(_), Some(yieldColumns)) => yieldColumns
              // else
              case _ => Seq.empty
            }
            val outgoing = incoming.amendedWith(resultColumns.toSet)
            val declared = Declarations(Seq.empty, resultColumns)
            incoming.noResultScope(outgoing, children, referenced, declared)

          // Non-local callable
          case None =>
            (isStandalone, declaredResult) match {
              // In standalone context
              case (true, None) =>
                // no YIELD and YIELD *
                incoming.resultScope(incoming, TableResultWithNotYetKnownColumns, children, referenced)
              case (true, Some(procedureResult)) =>
                // concrete YIELD a, b, ...
                val resultColumns = procedureResult.items.map(_.variable)
                val outgoing = incoming.amendedWith(resultColumns.toSet)
                val yieldWhereScopeOpt = declaredResult.flatMap(_.where.map(_.expression)).map(yW =>
                  pegExpression(yW, outgoing.constantChildContext())
                )
                incoming.resultScope(
                  outgoing,
                  TableResult(resultColumns),
                  children ++ yieldWhereScopeOpt,
                  referenced,
                  Declarations(Seq.empty, resultColumns)
                )

              // In inlined context
              case (false, None) =>
                // no YIELD  // TODO: This should be a NoResult, too
                incoming.omittedResultScope(incoming, children, referenced)
              case (false, Some(procedureResult)) =>
                // concrete YIELD a, b, ...
                val resultColumns = procedureResult.items.map(_.variable)
                val outgoing = incoming.amendedWith(resultColumns.toSet)
                val yieldWhereScopeOpt = declaredResult.flatMap(_.where.map(_.expression)).map(yW =>
                  pegExpression(yW, outgoing.constantChildContext())
                )
                incoming.noResultScope(
                  outgoing,
                  children ++ yieldWhereScopeOpt,
                  referenced,
                  Declarations(Seq.empty, resultColumns)
                )
            }
        }
      // resolved local named call
      case ResolvedLocalCall(_, _, outputSignature, _, callArguments, callResults, _, _, _, _) =>
        val children = callArguments.map(arg => pegExpression(arg, incoming.constantChildContext()))
        val referenced = Some(WorkingScope.referencedInChildren(children))
        val resultColumns = callResults.map(_.variable)
        val outgoing = incoming.amendedWith(resultColumns.toSet)
        val declared = Declarations(Seq.empty, resultColumns)

        incoming.noResultScope(outgoing, children, referenced, declared)

      // resolved non-local named call
      case ResolvedNonLocalCall(signature, callArguments, callResults, _, declaredResults, yieldAll, _) =>
        val children = callArguments.map(arg => pegExpression(arg, incoming.constantChildContext()))
        val referenced = Some(WorkingScope.referencedInChildren(children))

        val (declared, result) = signature.outputSignature match {
          case Some(_) =>
            val resultColumns = if (declaredResults || yieldAll) callResults.map(_.variable) else Seq.empty
            (Declarations(constants = Seq.empty, variables = resultColumns), TableResult(resultColumns))
          case None => (Declarations.noDeclarations, OmittedResult)

        }

        incoming.resultScope(incoming.amendedWith(declared.variables.toSet), result, children, referenced, declared)

      // query clauses
      case Unwind(expression, variable) =>
        val children = Seq(pegExpression(expression, incoming.constantChildContext()))
        val declared = Declarations(Seq.empty, Seq(variable))
        incoming.noResultScope(outgoing = incoming.amendedWith(variable), children, declared = declared)

      case Match(_, _, pattern, hints, whereOpt, searchOpt) =>
        val patternScope = pegPattern(pattern, incoming.constantChildContext(), foreachIterVar = None)
        val patternOutgoing = incoming.amendedWith(patternScope.outgoing.variables)

        val searchScopeOpt = searchOpt.map(search => scopeSearchSubclause(search, patternOutgoing))
        val searchOutgoing = if (searchScopeOpt.isDefined) searchScopeOpt.get.outgoing else patternOutgoing

        val hintsScopes = hints.flatMap(h => h.variables.map(pegExpression(_, searchOutgoing.constantChildContext())))
        val whereScopeOpt =
          whereOpt.map(where => pegExpression(where.expression, searchOutgoing.constantChildContext()))

        val children = Seq(Some(patternScope), searchScopeOpt, hintsScopes, whereScopeOpt).flatten
        val referenced = Some(WorkingScope.referencedInChildren(children) intersect incoming.constantsAndVariables)
        val declared = patternScope.declared
        val outgoing =
          incoming.amendedWith(
            patternScope.outgoing.variables diff incoming.constants
          ).amendedWith(searchScopeOpt.fold(Set.empty[LogicalVariable])(scope => scope.outgoing.variables))
        incoming.noResultScope(outgoing, children, referenced, declared)

      // load clause
      case LoadCSV(_, expression, variable, _) =>
        val children = Seq(pegExpression(expression, incoming.constantChildContext()))
        val declared = Declarations(Seq.empty, Seq(variable))
        incoming.noResultScope(outgoing = incoming.amendedWith(variable), children, declared = declared)

      // update clauses
      case Create(pattern) =>
        val patternScope = pegPattern(pattern, incoming.constantChildContext(), foreachIterVar)
        val children = Seq(patternScope)
        val declared = patternScope.declared
        val outgoing = incoming.amendedWithShadowingVariables(patternScope.outgoing.variables)
        incoming.omittedResultScope(outgoing, children, declared = declared)
      case Insert(pattern) =>
        val patternScope = pegPattern(pattern, incoming.constantChildContext(), foreachIterVar)
        val children = Seq(patternScope)
        val declared = patternScope.declared
        val outgoing = incoming.amendedWithShadowingVariables(patternScope.outgoing.variables)
        incoming.omittedResultScope(outgoing, children, declared = declared)

      case Merge(pattern, actions, whereOpt) =>
        val patternScope = pegPattern(pattern, incoming.constantChildContext(), foreachIterVar)
        // note that the `where` attribute of merge is only populated by rewriters
        // but is populated with predicate that see the variable bound by the pattern
        val inner = incoming.amendedWithShadowingVariables(patternScope.outgoing.variables)
        val whereScopeOpt = whereOpt.map(where =>
          pegExpression(where.expression, inner.constantChildContext())
        )
        val actionsScoped = actions.map(ma => apply(ma.action, inner, foreachIterVar))
        val children = Seq(Some(patternScope), actionsScoped, whereScopeOpt).flatten
        val declared = patternScope.declared
        incoming.omittedResultScope(
          incoming.amendedWithShadowingVariables(patternScope.outgoing.variables),
          children,
          declared = declared
        )

      case SetClause(items) =>
        val expressionIncoming = incoming.constantChildContext()
        val children = items.flatMap {
          case SetLabelItem(variable, _, dynamicLabels, _) => Seq(pegExpression(variable, expressionIncoming)) ++
              dynamicLabels.map(dl => pegExpression(dl, expressionIncoming))
          case SetPropertyItem(property, expression) =>
            Seq(
              pegExpression(property, expressionIncoming),
              pegExpression(expression, expressionIncoming)
            )
          case SetPropertyItems(container, items) =>
            pegExpression(container, expressionIncoming) +: items.map { case (_, expression) =>
              pegExpression(expression, expressionIncoming)
            }
          case SetExactPropertiesFromMapItem(variable, expression, _) =>
            Seq(
              pegExpression(variable, expressionIncoming),
              pegExpression(expression, expressionIncoming)
            )
          case SetIncludingPropertiesFromMapItem(variable, expression, _) =>
            Seq(
              pegExpression(variable, expressionIncoming),
              pegExpression(expression, expressionIncoming)
            )
          case SetDynamicPropertyItem(dynamicPropertyLookup, expression) =>
            Seq(
              pegExpression(dynamicPropertyLookup, expressionIncoming),
              pegExpression(expression, expressionIncoming)
            )
        }
        incoming.forwardWithOmittedResult(children)

      case Remove(items) =>
        val expressionIncoming = incoming.constantChildContext()
        val children = items.flatMap {
          case RemoveLabelItem(variable, _, dynamicLabels, _) =>
            Seq(pegExpression(variable, expressionIncoming)) ++
              dynamicLabels.map(dl => pegExpression(dl, expressionIncoming))
          case RemovePropertyItem(property) => Seq(pegExpression(property, expressionIncoming))
          case RemoveDynamicPropertyItem(dynamicPropertyLookup) =>
            Seq(pegExpression(dynamicPropertyLookup, expressionIncoming))
        }
        incoming.forwardWithOmittedResult(children)

      case Delete(items, _) =>
        val expressionIncoming = incoming.constantChildContext()
        val children = items.map(item => pegExpression(item, expressionIncoming))
        incoming.forwardWithOmittedResult(children)

      case Foreach(variable, expression, updates) =>
        val expressionIncoming = incoming
        val expressionScope = pegExpression(expression, expressionIncoming.constantChildContext())
        val subqueryScope =
          pegStatement(
            SingleQuery(updates)(updates.head.position),
            expressionIncoming.shadowVariable(variable),
            foreachIterVar = Some(variable)
          )
        val children = Seq(expressionScope, subqueryScope)
        val referenced = {
          val subqueryReferenced = subqueryScope.referenced diff variable
          val expressionReferenced = expressionScope.referenced
          Some(subqueryReferenced union expressionReferenced)
        }
        val declared = Declarations(Seq(variable), Seq.empty)
        incoming.omittedResultScope(outgoing = incoming, children, referenced, declared)

      // result/projection clauses
      case Finish() =>
        val children = WorkingScope.noChildren
        incoming.omittedResultScope(RegularContext.unit, children)

      // InputDataStream is not implemented in the parser
      case InputDataStream(variables) =>
        val vars = variables.toSet[LogicalVariable]
        incoming.noResultScope(incoming.amendedWith(vars), Seq.empty, declared = Declarations(Seq.empty, variables))

      case UseGraph(graphReference) =>
        incoming.noResultScope(incoming, Seq(pegExpression(graphReference, incoming.constantChildContext())))

      case projectionClause: ProjectionClause => scopeProjectionClause(projectionClause, incoming)

      case command: CommandClause => pegCommand(command, incoming)

      /**
       * To make match exhaustive
       */
      case _ => UnexpectedAstNodeScopingError(astNode, incoming)
    }
  }

  private def recognizeExpression(
    expression: Expression,
    incoming: RegularContext
  )(implicit c: PegContext): WorkingScope = {

    val updatedIncoming =
      if (c.language != CypherVersion.Cypher5 && expression.containsAggregate)
        incoming.isSubclauseAggregation
      else if (!expression.containsAggregate)
        incoming.subclauseChildContext
      else
        incoming.aggregatingConstantChildContext

    updatedIncoming.recognizeExpression(expression, isSubExpression = false) match {
      case Some(item) => updatedIncoming.recognizedLeafScope(expression, item)
      case None       => pegExpression(expression, updatedIncoming)
    }
  }

  private def scopeProjectionItems(
    incoming: RegularContext,
    aggregatingExpressionContext: RegularContext,
    aggregationItems: Seq[ReturnItem],
    groupingItems: Seq[ReturnItem]
  )(implicit c: PegContext): Seq[WorkingScope] = {
    groupingItems.map(item => {
      incoming.recognizeExpression(item.expression, isSubExpression = false) match {
        case Some(recognised) => incoming.recognizedLeafScope(item.expression, recognised)
        case None             => pegExpression(item.expression, incoming)
      }
    }) ++
      aggregationItems.map(item => pegExpression(item.expression, aggregatingExpressionContext))
  }

  private def scopeGroupBy(
    groupBy: GroupBy,
    incoming: ProjectionExpressionContext,
    itemIncoming: ProjectionExpressionContext
  )(implicit c: PegContext): WorkingScope = {

    val spec = incoming.projectionSpecification

    def scopeGroupingElement(element: Expression): ExpressionScope = {
      val elementScoped = pegExpression(element, incoming).asInstanceOf[ExpressionScope]

      val referencedAliases = elementScoped.referenced.filterTargets(spec.aliases).getVariables

      val transitiveScopeReferences =
        referencedAliases
          .flatMap(spec.getUnderlyingExpression)
          .map(expr => pegExpression(expr, itemIncoming))
          .map(_.referenced)

      elementScoped.copy(referenced = elementScoped.referenced union transitiveScopeReferences)
    }

    val children = groupBy.groupingElements match {
      case ExplicitGroupingElements(elements) =>
        elements.map(scopeGroupingElement)
      case GroupingAll() =>
        spec.groupingKeys.map(key => pegExpression(key.expression, incoming)).toSeq
      case GroupingNone() => Seq()
    }

    StatementScope(
      groupBy,
      incoming,
      WorkingScope.referencedInChildren(children),
      Declarations.noDeclarations,
      incoming,
      NoResult,
      children
    )
  }

  private def scopeSubclauses(
    incoming: RegularContext,
    subclauses: Subclauses
  )(implicit c: PegContext): Seq[WorkingScope] = {

    val Subclauses(_, orderByOpt, skipOpt, limitOpt, whereOpt) = subclauses

    val sortItemScopes =
      orderByOpt.fold(Seq.empty[SortItem])(_.sortItems)
        .map(item => recognizeExpression(item.expression, incoming))

    val whereExpScopes =
      whereOpt.toSeq.map(where => recognizeExpression(where.expression, incoming))

    val skipExpScopes =
      skipOpt.toSeq.map(skip => recognizeExpression(skip.expression, incoming))

    val limitExpScopes =
      limitOpt.toSeq.map(limit => recognizeExpression(limit.expression, incoming))

    sortItemScopes ++ skipExpScopes ++ limitExpScopes ++ whereExpScopes
  }

  private def getIntroducedVariables(
    incoming: RegularContext,
    variableItems: Seq[ReturnItem],
    items: Seq[ReturnItem],
    clauseType: ClauseType,
    projectionType: ProjectionType
  ): Seq[LogicalVariable] = {
    val availableItems = clauseType match {
      case DefaultYield if projectionType != FreeProjection => incoming.variables.map(AliasedReturnItem(_)).toSeq
      case DefaultWith | AddedInRewriteGeneral(_)           => variableItems
      case _                                                => items
    }

    returnItemAliases(availableItems)

  }

  private def getIncludedIncomingVariables(
    visibleIncomingVariables: Set[LogicalVariable],
    projectionType: ProjectionType
  ): Set[LogicalVariable] = projectionType match {
    case FreeProjection => Set.empty
    case _              => visibleIncomingVariables
  }

  private def getProjectionOutgoing(
    incomingConstants: Set[LogicalVariable],
    resultingVariables: Seq[LogicalVariable],
    constantItems: Seq[ReturnItem],
    clauseType: ClauseType,
    incomingLocalCallables: Set[LocalCallableScopeSignature]
  ): RegularContext = {

    val (constants, variables, localCallables) = clauseType match {
      case _: ReturnType =>
        (
          unitVariables,
          resultingVariables.filterNot(v =>
            constantItems.exists(_.name == v.name)
          ).toSet,
          Set.empty[LocalCallableScopeSignature]
        )
      case _: YieldType =>
        (unitVariables, resultingVariables.toSet, incomingLocalCallables)
      case _: WithType =>
        (incomingConstants, resultingVariables.toSet, incomingLocalCallables)
    }

    RegularContext(constants, variables, localCallables)
  }

  private def getProjectionResult(
    resultingVariables: Seq[LogicalVariable],
    clauseType: ClauseType
  ): Result = clauseType match {
    case _: YieldType | _: ReturnType => TableResult(resultingVariables)
    case _: WithType                  => NoResult
  }

  private def getProjectionDeclared(
    introducedVariables: Seq[LogicalVariable],
    clauseType: ClauseType,
    passThroughItems: Seq[ReturnItem]
  ): Declarations = clauseType match {
    case _: YieldType | _: ReturnType =>
      Declarations.noDeclarations
    case ParsedAsLet =>
      Declarations(Seq.empty, introducedVariables)
    case _: WithType =>
      Declarations(Seq.empty, introducedVariables.filterNot(passThroughItems.map(_.name) contains _.name))
  }

  private def getProjectionReferenced(
    children: Seq[WorkingScope],
    includedIncomingVariables: Set[LogicalVariable],
    incomingConstants: Set[LogicalVariable],
    clauseType: ClauseType,
    hasSideEffect: Boolean,
    incoming: RegularContext
  ): References = {
    val referencedInChildren = WorkingScope.referencedInChildren(children)
    val allRefs = (clauseType, hasSideEffect) match {
      case (_: WithType, false) => referencedInChildren
      case (_: WithType, true) =>
        val shadowedConstantNames = incoming.variables.iterator.map(_.name).toSet
        val effectiveConstants = incomingConstants.filterNot(c => shadowedConstantNames.contains(c.name))
        referencedInChildren union References.connect(
          (includedIncomingVariables union effectiveConstants).toSeq,
          incoming.allSymbols
        )
      case _ => referencedInChildren union References.connect(includedIncomingVariables.toSeq, incoming.allSymbols)
    }

    allRefs intersectByTarget incoming.allSymbols
  }

  private def scopeProjectionClause(
    projectionClause: ProjectionClause,
    incoming: RegularContext
  )(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = projectionClause

    val Elements(distinct, items, subclauses, clauseType, projectionType) = Elements(projectionClause)

    // partitions item "c" and "c AS c" where c is a constant — a special case Cypher historically allows
    // for WITH-style projections. LET is strict: every alias is a fresh declaration, no partition.
    val (constantItems, variableItems) =
      if (clauseType == ParsedAsLet) (Seq.empty[ReturnItem], items)
      else items.partition(ri => ri.alias.exists(v => ri.isPassThrough && (incoming.constants contains v)))

    val passThroughItems = items.filter(_.isPassThrough)

    val introducedVariables = getIntroducedVariables(incoming, variableItems, items, clauseType, projectionType)
    val visibleIncomingVariables = incoming.variables.filterNot(v => introducedVariables contains v)
    val includedIncomingVariables = getIncludedIncomingVariables(visibleIncomingVariables, projectionType)
    val resultingVariables = includedIncomingVariables.toSeq ++ introducedVariables

    val incomingByName: Map[String, LogicalVariable] =
      incoming.allSymbolsAndKeys.iterator.map(v => v.name -> v).toMap
    def remapPassThroughAlias(item: ReturnItem): ReturnItem = item match {
      case ari: AliasedReturnItem if ari.isPassThrough =>
        incomingByName.get(ari.variable.name) match {
          case Some(incomingVar) => AliasedReturnItem(ari.expression, incomingVar)(ari.position)
          case None              => ari
        }
      case other => other
    }
    val remappedItems = items.map(remapPassThroughAlias)

    val projectionItems =
      remappedItems.map(ri => (ri.expression, ri.alias)) ++ includedIncomingVariables.map(iiv =>
        iiv.copyId -> Some(iiv)
      )

    val (aggregatingItems, groupingItems) = remappedItems.partition(_.directlyContainsAggregate)

    val isAggregating = aggregatingItems.nonEmpty || distinct || subclauses.groupBy.isDefined
    val hasSideEffects = isAggregating || subclauses.hasSubclause

    val projectionSpecification =
      if (!isAggregating)
        ProjectionSpecification.nonAggregating(projectionItems)
      else if (subclauses.groupBy.isDefined)
        ProjectionSpecification.explicitKeys(subclauses.groupBy.get.groupingElements, projectionItems, distinct)
      else
        ProjectionSpecification.implicitKeys(projectionItems, distinct)

    val ProjectionContexts(nonAggregatingScope, aggregatingScope, groupByScope, subclauseScope) =
      if (isAggregating)
        aggregatingProjectionContexts(incoming, projectionSpecification, clauseType, introducedVariables)
      else nonAggregatingProjectionContexts(incoming, projectionSpecification, clauseType, introducedVariables, items)

    val itemScopes =
      scopeProjectionItems(nonAggregatingScope, aggregatingScope, aggregatingItems, groupingItems)

    val groupByScopeOpt =
      subclauses.groupBy.map(gb =>
        scopeGroupBy(
          gb,
          groupByScope,
          incoming.amendedWithProjectionSpecification(projectionSpecification, GroupByPart)
        )
      )

    val subclauseScopes = scopeSubclauses(subclauseScope, subclauses)

    val children = itemScopes ++ groupByScopeOpt ++ subclauseScopes
    val outgoingInter =
      getProjectionOutgoing(incoming.constants, resultingVariables, constantItems, clauseType, incoming.localCallables)
    val passThroughNames: Set[String] = passThroughItems.flatMap(_.alias).map(_.name).toSet
    val outgoing = outgoingInter.replaceWith(outgoingInter.variables.map(v =>
      if (passThroughNames.contains(v.name))
        incoming.allSymbolsAndKeys.find(_.name == v.name).getOrElse(v)
      else v
    ))
    val result = getProjectionResult(resultingVariables, clauseType)
    val declared = getProjectionDeclared(introducedVariables, clauseType, passThroughItems)
    val referenced =
      getProjectionReferenced(
        children,
        includedIncomingVariables,
        incoming.constants,
        clauseType,
        hasSideEffects,
        incoming
      ) union References.connect(passThroughItems.flatMap(_.alias), incoming.allSymbolsAndKeys)

    StatementScope(astNode, incoming, referenced, declared, outgoing, result, children)

  }

  private case class ProjectionContexts(
    nonAggregatingScope: ProjectionExpressionContext,
    aggregatingScope: ProjectionExpressionContext,
    groupByScope: ProjectionExpressionContext,
    subclauseScope: ProjectionExpressionContext
  )

  private def aggregatingProjectionContexts(
    incoming: RegularContext,
    projectionSpecification: ProjectionSpecification,
    clauseType: ClauseType,
    introducedVariables: Seq[LogicalVariable]
  ): ProjectionContexts = {
    val itemIncoming = incoming.amendedWithProjectionSpecification(projectionSpecification, AggregatingPart)
    val groupByIncoming =
      incoming.amendedWithProjectionSpecification(projectionSpecification, GroupByPart).groupByContext()
    val subclauseIncoming = (clauseType match {
      case DefaultYield => incoming.replaceWith(introducedVariables.toSet)
      case _            => incoming
    }).amendedWithProjectionSpecification(projectionSpecification, NonAggregatingSubclausePart)

    ProjectionContexts(
      nonAggregatingScope = itemIncoming.nonAggregatingChildContext(),
      aggregatingScope = itemIncoming,
      groupByScope = groupByIncoming,
      subclauseScope = subclauseIncoming
    )
  }

  private def nonAggregatingProjectionContexts(
    incoming: RegularContext,
    spec: ProjectionSpecification,
    clauseType: ClauseType,
    introducedVariables: Seq[LogicalVariable],
    items: Seq[ReturnItem]
  ): ProjectionContexts = {
    val subclauseIncoming = (clauseType match {
      case DefaultYield => incoming.replaceWith(introducedVariables.toSet)
      case _            => incoming
    }).amendedWithShadowingVariables(items.flatMap(_.alias).toSet).constantChildContext()

    ProjectionContexts(
      incoming.constantChildContext().amendedWithProjectionSpecification(spec, NonAggregatingPart),
      incoming.amendedWithProjectionSpecification(spec, AggregatingPart),
      incoming.amendedWithProjectionSpecification(spec, GroupByPart),
      subclauseIncoming.amendedWithProjectionSpecification(spec, NonAggregatingSubclausePart)
    )
  }

  private def returnItemAliases(items: Seq[ReturnItem]): Seq[LogicalVariable] =
    items.map(item => item.alias.getOrElse(UnPositionedVariable.varFor(item.name)))

  private def scopeInlineSubquery(
    callClause: SubqueryCall,
    incoming: RegularContext,
    explicitlyImportedVariables: Set[LogicalVariable],
    innerQueryIncoming: RegularContext,
    inTransactionsParameters: Option[InTransactionsParameters]
  )(implicit c: PegContext) = {
    implicit val astNode: ASTNode = callClause

    val innerQueryScope =
      pegStatement(callClause.innerQuery, innerQueryIncoming, callClause.isInstanceOf[ImportingWithSubqueryCall])
    val (inTransactionsChildren, declaredInTransactionsVariables) =
      scopeInTransactionParameters(inTransactionsParameters, incoming.constantChildContext(), innerQueryIncoming)
    val (outgoing, declaredVariables) = innerQueryScope.result match {
      case TableResult(columns) => (incoming.amendedWith((columns ++ declaredInTransactionsVariables).toSet), columns)
      case TableResultWithNotYetKnownColumns => (incoming.amendedWith(declaredInTransactionsVariables.toSet), Seq.empty)
      case OmittedResult                     => (incoming.amendedWith(declaredInTransactionsVariables.toSet), Seq.empty)
      case NoResult                          =>
        // subquery does not end with the right clause,
        // so this is a best effort:
        (incoming.amendedWith(declaredInTransactionsVariables.toSet), Seq.empty)
      case ExpressionResult =>
        throw new IllegalStateException("inner query cannot have an expression result")
    }
    val children = innerQueryScope +: inTransactionsChildren

    val referenced = Some(References.connectOrDrop(
      (innerQueryScope.referenced.getVariables ++ explicitlyImportedVariables).toSeq,
      incoming.allSymbols
    ))
    val declared = Declarations(Seq.empty, declaredVariables ++ declaredInTransactionsVariables)
    incoming.noResultScope(outgoing, children, referenced, declared)
  }

  private def scopeInTransactionParameters(
    inTransactionsParameters: Option[InTransactionsParameters],
    incoming: RegularContext,
    innerQueryIncoming: RegularContext
  )(implicit c: PegContext)
    : (Seq[WorkingScope], Seq[LogicalVariable]) = {
    inTransactionsParameters match {
      case None => (Seq.empty, Seq.empty)
      case Some(InTransactionsParameters(
          batchParams,
          concurrencyParams,
          errorParams,
          reportParams,
          disjointByParams
        )) =>
        val batchParamsChild = batchParams.toSeq.map {
          case InTransactionsBatchParameters(batchSize) =>
            pegExpression(batchSize, incoming)
        }
        val concurrencyParamsChild = concurrencyParams.toSeq.flatMap {
          case InTransactionsConcurrencyParameters(Some(concurrency)) =>
            Some(pegExpression(concurrency, incoming))
          case _ => None
        }
        val errorParamsChild = errorParams.toSeq.flatMap {
          case InTransactionsErrorParameters(_, Some(InTransactionsRetryParameters(Some(timeout)))) =>
            Some(pegExpression(timeout, incoming))
          case _ => None
        }
        // NOTE: DISJOINT BY expressions are evaluated in the inner scope of the CALL subquery, so only imported variables are visible.
        val disjointByParamsChild = disjointByParams.toSeq.flatMap {
          case InTransactionsDisjointByParameters(InTransactionsDisjointByMode.DisjointByExpressions(expressions)) =>
            expressions.map(expr => pegExpression(expr, innerQueryIncoming))
          case _ => Seq.empty
        }
        val reportVariable = reportParams.toSeq.map {
          case InTransactionsReportParameters(reportAs) => reportAs
        }
        (batchParamsChild ++ concurrencyParamsChild ++ errorParamsChild ++ disjointByParamsChild, reportVariable)
    }
  }

  private def scopeSearchSubclause(
    search: Search,
    incoming: RegularContext
  )(implicit c: PegContext): StatementScope = {
    implicit val astNode: ASTNode = search

    val constantIncoming = incoming.constantChildContext()

    val bindingVariable = search.bindingVariable
    val bindingVariableScope = pegExpression(bindingVariable, constantIncoming)
    val embedding = search.embedding
    val embeddingScope = pegExpression(embedding, constantIncoming)
    val whereOpt = search.where
    val whereScopeOpt = whereOpt.map(where =>
      pegExpression(where.expression, incoming.constantChildContext())
    )
    val limit = search.limit
    val limitScope = pegExpression(limit.expression, constantIncoming)
    val scoreOpt = search.score

    val outgoing = if (scoreOpt.isDefined) incoming.amendedWith(scoreOpt.get) else incoming

    val children: Seq[WorkingScope] =
      Seq(Some(bindingVariableScope), Some(embeddingScope), whereScopeOpt, Some(limitScope)).flatten

    val referenced = Some(WorkingScope.referencedInChildren(children) intersect incoming.constantsAndVariables)

    incoming.noResultScope(outgoing, children, referenced, declared = Declarations(Seq.empty, scoreOpt.toSeq))

  }
}
