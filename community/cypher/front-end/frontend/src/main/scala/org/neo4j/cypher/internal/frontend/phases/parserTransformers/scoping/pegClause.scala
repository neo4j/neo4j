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

import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ClauseType
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.DefaultYield
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.InputDataStream
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ProjectionClause.Elements
import org.neo4j.cypher.internal.ast.ProjectionClause.Subclauses
import org.neo4j.cypher.internal.ast.ProjectionType
import org.neo4j.cypher.internal.ast.Query
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
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.WithType
import org.neo4j.cypher.internal.ast.YieldType
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor.unitVariables
import org.neo4j.cypher.internal.util.ASTNode

object pegClause {

  def apply(clause: Clause, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[Clause](clause, incoming, applyUncached(_, _))
  }

  private def applyUncached(clause: Clause, incoming: RegularContext)(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = clause
    clause match {

      /**
       * Clause
       */
      case call @ ImportingWithSubqueryCall(query, inTransactionsParameters, _) =>
        val importingAll = query.isCorrelated && query.importColumns.isEmpty
        val explicitImportedVariables = query.importColumns.toSet
        val importedVariableSet: Set[LogicalVariable] =
          if (importingAll) incoming.allSymbols else explicitImportedVariables
        val graphSelectionScopes = query.getGraphSelections.map(gs =>
          pegExpression(gs.graphReference, incoming.constantChildContext())
        )
        val innerQueryIncoming =
          RegularContext(constants = unitVariables, variables = importedVariableSet, incoming.localCallables)
        val innerQuery = query.withoutImportingWithAndGraphSelection
        val scope = if (innerQuery.isDefined) scopeInlineSubquery(
          call,
          incoming,
          explicitImportedVariables,
          innerQueryIncoming,
          innerQuery.get,
          inTransactionsParameters
        )
        else StatementScope(call, incoming, Set.empty, Declarations.noDeclarations, RegularContext.unit)
        scope.withChildren(scope.children ++ graphSelectionScopes)

      case call @ ScopeClauseSubqueryCall(innerQuery, isImportingAll, importedVariables, inTransactionsParameters, _) =>
        val innerQueryIncoming =
          if (isImportingAll) incoming.constantChildContext()
          else RegularContext(
            constants = importedVariables.toSet,
            variables = unitVariables,
            localCallables = incoming.localCallables
          )

        scopeInlineSubquery(
          call,
          incoming,
          importedVariables.toSet,
          innerQueryIncoming,
          innerQuery,
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
          case lc: LocalCallableScopeSignature if lc.name == procedureName => lc
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
          case Some(LocalCallableScopeSignature(_, procedureResult)) =>
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
      // resolved named call
      case ResolvedCall(signature, callArguments, callResults, _, _, _, _) =>
        val children = callArguments.map(arg => pegExpression(arg, incoming.constantChildContext()))
        val referenced = Some(WorkingScope.referencedInChildren(children))

        val (declared, result) = signature.outputSignature match {
          case Some(_) =>
            val resultColumns = callResults.map(_.variable)
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
        val patternScope = pegPattern(pattern, incoming.constantChildContext())
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
        val patternScope = pegPattern(pattern, incoming.constantChildContext())
        val children = Seq(patternScope)
        val declared = patternScope.declared
        val outgoing = incoming.amendedWith(patternScope.outgoing.variables)
        incoming.omittedResultScope(outgoing, children, declared = declared)
      case Insert(pattern) =>
        val patternScope = pegPattern(pattern, incoming.constantChildContext())
        val children = Seq(patternScope)
        val declared = patternScope.declared
        val outgoing = incoming.amendedWith(patternScope.outgoing.variables)
        incoming.omittedResultScope(outgoing, children, declared = declared)

      case Merge(pattern, actions, whereOpt) =>
        val patternScope = pegPattern(pattern, incoming.constantChildContext())
        // note that the `where` attribute of merge is only populated by rewriters
        // but is populated with predicate that see the variable bound by the pattern
        val inner = incoming.amendedWith(patternScope.outgoing.variables)
        val whereScopeOpt = whereOpt.map(where =>
          pegExpression(where.expression, inner.constantChildContext())
        )
        val actionsScoped = actions.map(ma => apply(ma.action, inner))
        val children = Seq(Some(patternScope), actionsScoped, whereScopeOpt).flatten
        val declared = patternScope.declared
        incoming.omittedResultScope(
          incoming.amendedWith(patternScope.outgoing.variables),
          children,
          declared = declared
        )

      case SetClause(items) =>
        val expressionIncoming = incoming.constantChildContext()
        val children = items.flatMap {
          case SetLabelItem(variable, _, _, _) => Seq(pegExpression(variable, expressionIncoming))
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
          case RemoveLabelItem(variable, _, _, _) =>
            Seq(pegExpression(variable, expressionIncoming))
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
            expressionIncoming.amendedWith(variable)
          )
        val children = Seq(expressionScope, subqueryScope)
        val referenced = {
          val subqueryReferenced = subqueryScope.referenced excl variable
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

      // TODO other clause, specifically admin clauses

      case command: CommandClause => pegCommand(command, incoming)

      /**
       * To make match exhaustive
       */
      case _ => UnexpectedAstNodeScopingError(astNode, incoming)
    }
  }

  private def getExpressionSet(items: Seq[ReturnItem]): Set[Expression] =
    items.map(ri => ri.expression).toSet

  private def getIncoming(
    incoming: RegularContext,
    introducedVariables: Seq[LogicalVariable],
    visibleIncomingVariables: Set[LogicalVariable],
    clauseType: ClauseType
  ): (RegularContext, RegularContext) = clauseType match {
    case DefaultYield =>
      (incoming, incoming.replaceWith(introducedVariables.toSet).constantChildContext())
    case _ =>
      (incoming, incoming.replaceWith(introducedVariables.toSet union visibleIncomingVariables).constantChildContext())
  }

  private def getAggregationIncoming(
    incoming: RegularContext,
    groupingItems: Seq[ReturnItem],
    includedIncomingVariables: Set[LogicalVariable],
    introducedVariables: Seq[LogicalVariable]
  ): (RegularContext, RegularContext) = {
    val groupingKeysItems = getExpressionSet(groupingItems) ++ includedIncomingVariables

    (
      incoming.amendedWithGroupingKeys(groupingKeysItems, inSubclause = false),
      incoming
        .replaceWith(Set.empty)
        .amendedWithGroupingKeys(groupingKeysItems union introducedVariables.toSet, inSubclause = true)
    )
  }

  private def scopeProjectionItems(
    incoming: RegularContext,
    aggregatingExpressionContext: RegularContext,
    aggregationItems: Seq[ReturnItem],
    groupingItems: Seq[ReturnItem]
  )(implicit c: PegContext): Seq[WorkingScope] = {

    groupingItems.map(item => pegExpression(item.expression, incoming.constantChildContext())) ++
      aggregationItems.map(item => pegExpression(item.expression, aggregatingExpressionContext))
  }

  private def scopeSubclauses(
    incoming: RegularContext,
    subclauses: Subclauses
  )(implicit c: PegContext): Seq[WorkingScope] = {

    val Subclauses(orderByOpt, skipOpt, limitOpt, whereOpt) = subclauses

    val sortItemScopes =
      orderByOpt.fold(Seq.empty[SortItem])(_.sortItems)
        .map(item => pegExpression(item.expression, incoming))

    val whereExpScopes =
      whereOpt.toSeq.map(where => pegExpression(where.expression, incoming))

    val skipExpScopes =
      skipOpt.toSeq.map(skip => pegExpression(skip.expression, incoming))

    val limitExpScopes =
      limitOpt.toSeq.map(limit => pegExpression(limit.expression, incoming))

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
      case DefaultWith | AddedInRewriteGeneral              => variableItems
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
    clauseType: ClauseType
  ): Declarations = clauseType match {
    case _: YieldType | _: ReturnType =>
      Declarations.noDeclarations
    case _: WithType =>
      Declarations(Seq.empty, introducedVariables)
  }

  private def getProjectionReferenced(
    children: Seq[WorkingScope],
    includedIncomingVariables: Set[LogicalVariable],
    incomingConstants: Set[LogicalVariable],
    clauseType: ClauseType,
    hasSideEffect: Boolean
  ): Set[LogicalVariable] = {
    val referencedInChildren = WorkingScope.referencedInChildren(children)
    (clauseType, hasSideEffect) match {
      case (_: WithType, false) => referencedInChildren
      case (_: WithType, true)  => referencedInChildren union includedIncomingVariables union incomingConstants
      case _                    => referencedInChildren union includedIncomingVariables
    }
  }

  private def scopeProjectionClause(
    projectionClause: ProjectionClause,
    incoming: RegularContext
  )(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = projectionClause

    val Elements(distinct, items, subclauses, clauseType, projectionType) = Elements(projectionClause)

    // partitions item "c" and "c AS c" where c is a constant — a special case Cypher historically allows
    val (constantItems, variableItems) =
      items.partition(ri => ri.alias.exists(v => ri.isPassThrough && (incoming.constants contains v)))

    val introducedVariables = getIntroducedVariables(incoming, variableItems, items, clauseType, projectionType)
    val visibleIncomingVariables = incoming.variables.filterNot(v => introducedVariables contains v)
    val includedIncomingVariables = getIncludedIncomingVariables(visibleIncomingVariables, projectionType)
    val resultingVariables = includedIncomingVariables.toSeq ++ introducedVariables

    val (aggregatingItems, groupingItems) = items.partition(_.directlyContainsAggregate)
    val isAggregating = aggregatingItems.nonEmpty || distinct
    val hasSideEffects = isAggregating || subclauses.hasSubclause

    val (itemIncoming, subclauseIncoming) =
      if (isAggregating)
        getAggregationIncoming(incoming, groupingItems, includedIncomingVariables, introducedVariables)
      else
        getIncoming(incoming, introducedVariables, visibleIncomingVariables, clauseType)

    val itemScopes = scopeProjectionItems(incoming, itemIncoming, aggregatingItems, groupingItems)

    val subclauseScopes = scopeSubclauses(subclauseIncoming, subclauses)

    val children = itemScopes ++ subclauseScopes
    val outgoing =
      getProjectionOutgoing(incoming.constants, resultingVariables, constantItems, clauseType, incoming.localCallables)
    val result = getProjectionResult(resultingVariables, clauseType)
    val declared = getProjectionDeclared(introducedVariables, clauseType)
    val referenced =
      getProjectionReferenced(children, includedIncomingVariables, incoming.constants, clauseType, hasSideEffects)

    StatementScope(astNode, incoming, referenced, declared, outgoing, result, children)

  }

  @inline private def returnItemAliases(items: Seq[ReturnItem]): Seq[LogicalVariable] =
    items.map(item => item.alias.getOrElse(UnPositionedVariable.varFor(item.name)))

  @inline private def scopeInlineSubquery(
    callClause: SubqueryCall,
    incoming: RegularContext,
    explicitlyImportedVariables: Set[LogicalVariable],
    innerQueryIncoming: RegularContext,
    innerQuery: Query,
    inTransactionsParameters: Option[InTransactionsParameters]
  )(implicit c: PegContext) = {
    implicit val astNode: ASTNode = callClause

    val innerQueryScope = pegStatement(innerQuery, innerQueryIncoming)
    val (inTransactionsChildren, declaredInTransactionsVariables) =
      scopeInTransactionParameters(inTransactionsParameters, incoming.constantChildContext())
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
    val referenced = Some(innerQueryScope.referenced union explicitlyImportedVariables)
    val declared = Declarations(Seq.empty, declaredVariables ++ declaredInTransactionsVariables)
    incoming.noResultScope(outgoing, children, referenced, declared)
  }

  @inline private def scopeInTransactionParameters(
    inTransactionsParameters: Option[InTransactionsParameters],
    incoming: RegularContext
  )(implicit c: PegContext)
    : (Seq[WorkingScope], Seq[LogicalVariable]) = {
    inTransactionsParameters match {
      case None => (Seq.empty, Seq.empty)
      case Some(InTransactionsParameters(batchParams, concurrencyParams, errorParams, reportParams)) =>
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
        val reportVariable = reportParams.toSeq.map {
          case InTransactionsReportParameters(reportAs) => reportAs
        }
        (batchParamsChild ++ concurrencyParamsChild ++ errorParamsChild, reportVariable)
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
