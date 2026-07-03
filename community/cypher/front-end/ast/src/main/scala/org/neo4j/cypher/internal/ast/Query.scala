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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen.msg
import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.Scope.DeclarationsAndDependencies
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisToolingErrorWithGqlInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromFunctionWithContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromState
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.mapState
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.setState
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeZipper
import org.neo4j.cypher.internal.ast.semantics.Symbol
import org.neo4j.cypher.internal.ast.semantics._
import org.neo4j.cypher.internal.ast.semantics.iterableOnceSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.liftSemanticErrorDef
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.notification.SubqueryVariableShadowing
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME

import scala.annotation.tailrec

sealed trait QueryUtils {

  /**
   * All variables that are explicitly listed to be returned from this statement.
   * This also includes the information whether all other potentially existing variables in scope are also returned.
   */
  def returnVariables: ReturnVariables

  /**
   * All variables that are explicitly listed to be returned from this statement.
   */
  def returnColumns: List[LogicalVariable] = returnVariables.explicitVariables.toList

  /**
   * True iff this query part ends with a return clause.
   */
  def isReturning: Boolean

  /**
   * Given the root scope for this query part,
   * looks up the final scope after the last clause
   */
  def finalScope(scope: Scope): Scope

  /**
   * Semantic check for when this `Query` is the body of local callable, and imports
   * local callable parameters as variables from the `outer` scope
   */
  def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck

  /**
   * Semantic check for when this `Query` is in a subquery, and might import
   * variables from the `outer` scope
   */
  def semanticCheckInSubqueryContext(outer: SemanticState, current: SemanticState, optional: Boolean): SemanticCheck

  def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck

  /**
   * Exists and Count can omit the Return Statement
   * Count still requires it for Distinct Unions as in this case the count
   * changes based on which rows are distinct vs not
   */
  def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed = ImportingWithSubqueryCall
  ): SemanticCheck

  /**
   * Semantic check for when this `Query` is enclosed in outer context
   */
  def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck

  def importValuesFromRecordedFinalScope(query: Query): SemanticCheck = { (state: SemanticState) =>
    val scopeToImportFrom = state.scope(query).getOrElse(Scope.empty)
    SemanticCheckResult.success(state
      .importValuesFromScope(finalScope(scopeToImportFrom))
      .importValuesFromScope(scopeToImportFrom))
  }

}

sealed trait ResultEmitter extends Product

object ResultEmitter {
  case class SingleClause(clause: Clause) extends ResultEmitter

  case class OrEmitter(emitters: Seq[ResultEmitter]) extends ResultEmitter

  case class AndEmitter(emitters: Seq[ResultEmitter]) extends ResultEmitter
}

sealed trait Query extends Statement with SemanticCheckable with SemanticAnalysisTooling with QueryUtils {
  def containsUpdates: Boolean

  /**
   * All Return clauses contained within this statement.
   */
  def getReturns: Seq[Return]

  /**
   * True iff this query part ends with a finish clause.
   */
  def endsWithFinish: Boolean

  /**
   * Check this query part if it starts with an importing WITH
   */
  def checkImportingWith(optional: Boolean): SemanticCheck

  def invalidImportingWith: Seq[SemanticError]

  /**
   * True if this query part starts with an importing WITH (has incoming arguments)
   */
  def isCorrelated: Boolean

  /**
   * Returns names of variables imported using importing WITH
   */
  def importColumns: Seq[LogicalVariable]

  def getImportingWithItems: Seq[ReturnItem]

  /**
   * Returns the query stripped from importing WITH responsible for top-level importing and a USE graph clause.
   *
   * Example:
   *
   * USE neo4j
   * WITH x
   * RETURN x AS y
   * UNION
   * USE neo4j
   * WITH x
   * CALL { USE neo4j WITH x RETURN x AS y }
   * RETURN y
   *
   * returns as
   *
   * RETURN x AS y
   * UNION
   * CALL { WITH x RETURN x AS y }
   * RETURN y
   */
  def withoutImportingWithAndGraphSelection: Option[Query]

  def getGraphSelections: Seq[GraphSelection]

  /**
   * Return a copy of this query where the mapping function f is applied
   * to each returning single query, regardless if this a single query or a union query.
   */
  def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query

  /**
   * This collects the Return, FINISH, or update clauses that define the result or no result emitted by this query.
   */
  def getEmittingResultClauses: ResultEmitter

  final protected def checkNoDefineAndCallInTransactions: SemanticCheck = {
    val containsLocalDefinitions = folder.treeExists {
      case _: QueryWithLocalDefinitions => true
    }
    if (!containsLocalDefinitions) {
      SemanticCheck.success
    } else {
      SubqueryCall.findTransactionalSubquery(this).foldSemanticCheck { subqueryCall =>
        SemanticCheck.error(SemanticError.invalidUseOfDefineAndCIT(subqueryCall.position))
      }
    }
  }
}

sealed trait PartQuery extends Query {
  def clauses: Seq[Clause]

  def singleQuery: SingleQuery

  override def containsUpdates: Boolean =
    clauses.exists {
      case sub: SubqueryCall => sub.innerQuery.containsUpdates
      case call: CallClause  => !call.containsNoUpdates
      case _: UpdateClause   => true
      case _                 => false
    }

  override def returnVariables: ReturnVariables = clauses.last.returnVariables

  override def getReturns: Seq[Return] = {
    clauses.last match {
      case r: Return => Seq(r)
      case _         => Seq.empty
    }
  }

  override def isReturning: Boolean = clauses.last match {
    case _: Return => true
    case _         => false
  }

  override def endsWithFinish: Boolean = clauses.last match {
    case _: Finish => true
    case _         => false
  }

  override def finalScope(scope: Scope): Scope =
    if (scope.children.size < 1) Scope.empty else scope.children.last

  override def withoutImportingWithAndGraphSelection: Option[PartQuery]

  override def getGraphSelections: Seq[GraphSelection]
}

case class SingleQuery(clauses: Seq[Clause])(val position: InputPosition) extends PartQuery {
  assert(clauses.nonEmpty)

  override def singleQuery: SingleQuery = this

  private val getCommandClauses: Seq[CommandClause] =
    clauses.filter(_.isInstanceOf[CommandClause]).map(_.asInstanceOf[CommandClause])

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query = f(this)

  override def getEmittingResultClauses: ResultEmitter = ResultEmitter.SingleClause(clauses.last)

  private val partitionedClausesLazy: LazyVal[SingleQuery.PartitionedClauses] =
    LazyVal(SingleQuery.partitionClauses(clauses))
  def partitionedClauses: SingleQuery.PartitionedClauses = partitionedClausesLazy.value

  /**
   * The query is correlated if it imports variables from a parent query, this can happen if:
   *   - it contains an importing `WITH` clause (in first position or in second position right after a `USE` clause), or
   *   - it starts with a dynamic `USE` clause where the graph reference depends on a variable when invoking `graph.byName` or `graph.byElementId` (in a composite query).
   */
  override def isCorrelated: Boolean =
    partitionedClauses.importingWith.isDefined ||
      partitionedClauses.initialGraphSelection.exists(_.graphReference.dependencies.nonEmpty)

  override def importColumns: Seq[LogicalVariable] = partitionedClauses.importingWith match {
    case Some(w) => w.returnItems.items.map(item =>
        item.alias getOrElse { Variable(item.name)(item.position, Variable.isIsolatedDefault) }
      )
    case _ => Seq.empty
  }

  override def getImportingWithItems: Seq[ReturnItem] =
    partitionedClauses.importingWith.toSeq.flatMap(_.returnItems.items)

  override def withoutImportingWithAndGraphSelection: Option[SingleQuery] = {
    Option.when(
      partitionedClauses.clausesExceptImportingWithAndInitialGraphSelection.nonEmpty
    )(SingleQuery(partitionedClauses.clausesExceptImportingWithAndInitialGraphSelection)(position))
  }

  override def getGraphSelections: Seq[GraphSelection] =
    partitionedClauses.initialGraphSelection.toSeq

  private def leadingNonImportingWith: Option[With] =
    if (partitionedClauses.importingWith.isDefined)
      None
    else
      partitionedClauses.clausesExceptImportingWithAndLeadingGraphSelection.headOption match {
        case Some(nonImportingWith @ With(_, _, _, _, _, _, _, _: MayBeImportingWithType)) => Some(nonImportingWith)
        case _                                                                             => None
      }

  private def semanticCheckAbstractInScopeSubquery(
    clauses: Seq[Clause],
    clauseCheck: Seq[Clause] => SemanticCheck,
    canOmitReturnClause: Boolean = false,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    checkStandaloneCall(clauses) chain
      withScopedState(clauseCheck(clauses)) chain
      checkComposableNonTransactionCommandsAllowed(context) chain
      checkOrder(clauses, canOmitReturnClause, context) chain
      checkNoCallInTransactionsAfterWriteClause(clauses) chain
      checkInputDataStream(clauses) chain
      checkUsePositionInScopeSubquery() chain
      recordCurrentScope(this)

  private def semanticCheckAbstract(
    clauses: Seq[Clause],
    clauseCheck: Seq[Clause] => SemanticCheck,
    canOmitReturnClause: Boolean = false,
    context: UnaliasedNotAllowed = ImportingWithSubqueryCall
  ): SemanticCheck =
    checkStandaloneCall(clauses) chain
      withScopedState(clauseCheck(clauses)) chain
      checkComposableNonTransactionCommandsAllowed(context) chain
      checkOrder(clauses, canOmitReturnClause, context) chain
      checkNoCallInTransactionsAfterWriteClause(clauses) chain
      checkInputDataStream(clauses) chain
      checkUsePosition() chain
      checkUseForRoutedSystemCommand() chain
      recordCurrentScope(this)

  override def semanticCheck: SemanticCheck =
    checkNoDefineAndCallInTransactions chain
      semanticCheckAbstract(clauses, checkClauses(_, None))

  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck =
    semanticCheckAbstract(clauses, checkClauses(_, None, context), context = context)

  /**
   * No outer scope is needed for checkClauses as we don't need to check the naming of any returned variables
   * as no variables from EXISTS / COUNT can be returned, unlike in CALL subqueries.
   */
  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    semanticCheckAbstract(
      clauses,
      checkClauses(_, None, context),
      canOmitReturnClause = canOmitReturn,
      context = context
    )

  override def checkImportingWith(optional: Boolean): SemanticCheck =
    partitionedClauses.importingWith.foldSemanticCheck(_.semanticCheck)

  override def invalidImportingWith: Seq[SemanticError] = leadingNonImportingWith.map { wth =>
    def err(keyword: String): Seq[SemanticError] =
      Seq(SemanticError.invalidImportingWithKeyword(keyword, wth.position))

    val invalidValues = wth.returnItems.items.find(!_.isPassThrough)
    if (invalidValues.nonEmpty) {
      val value = invalidValues.head
      val aliasString = if (value.alias.nonEmpty) s" AS ${value.alias.get.name}" else ""
      val expression = ExpressionStringifier.apply().apply(value.expression)
      val input = expression + aliasString
      Seq(SemanticError.invalidImportingWithAliasOrExpression(input, wth.position))
    } else if (wth.distinct) {
      err("DISTINCT")
    } else if (wth.orderBy.isDefined) {
      err("ORDER BY")
    } else if (wth.where.isDefined) {
      err("WHERE")
    } else if (wth.skip.isDefined) {
      err("SKIP")
    } else if (wth.limit.isDefined) {
      err("LIMIT")
    } else Seq.empty[SemanticError]
  }.getOrElse(Seq.empty[SemanticError])

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck = {
    def importVariables: SemanticCheck =
      partitionedClauses.importingWith.foldSemanticCheck(wth =>
        wth.semanticCheckContinuation(outer.currentScope.scope) chain
          recordCurrentScope(wth)
      )

    val workingGraph = outer.workingGraph

    checkIllegalImportWith chain
      checkInitialGraphSelection(outer) chain
      semanticCheckAbstract(
        partitionedClauses.clausesExceptImportingWithAndInitialGraphSelection,
        importVariables chain checkClauses(_, Some(outer.currentScope.scope), ImportingWithSubqueryCall),
        context = ImportingWithSubqueryCall
      ) chain
      warnOnPotentiallyShadowVariables(outer, optional) chain
      SemanticCheck.fromState(state =>
        SemanticCheck.setState(state.recordWorkingGraph(workingGraph))
      ) // resetWorkingGraph
  }

  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckInSubqueryAbstract(outer, current, optional, withShadowedCheck = false, QueryWithLocalDefinitions)

  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckInSubqueryAbstract(outer, current, optional, withShadowedCheck = true, ScopeClauseSubqueryCall)

  private def semanticCheckInSubqueryAbstract(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean,
    withShadowedCheck: Boolean,
    context: UnaliasedNotAllowed
  ): SemanticCheck = {
    val workingGraph = outer.workingGraph

    checkInitialGraphSelection(outer) chain
      semanticCheckAbstractInScopeSubquery(
        partitionedClauses.clausesExceptInitialGraphSelection,
        checkClauses(_, Some(outer.currentScope.scope), context),
        context = context
      ) chain
      (if (withShadowedCheck) errorOnShadowedImportVariables(outer) else success) chain
      warnOnPotentiallyShadowVariables(current, optional) chain
      SemanticCheck.fromState(state =>
        SemanticCheck.setState(state.recordWorkingGraph(workingGraph))
      ) // resetWorkingGraph
  }

  private def checkInitialGraphSelection(outer: SemanticState): SemanticCheck =
    partitionedClauses.initialGraphSelection.foldSemanticCheck { graphSelection =>
      withState(outer)(graphSelection.semanticCheck)
    }

  private def checkIllegalImportWith: SemanticCheck = leadingNonImportingWith.foldSemanticCheck { wth =>
    def err(keyword: String): SemanticCheck =
      error(SemanticError.invalidImportingWithKeyword(keyword, wth.position))

    def checkReturnItems: SemanticCheck = {
      val invalidValues = wth.returnItems.items.find(!_.isPassThrough)
      when(invalidValues.nonEmpty) {
        val value = invalidValues.head
        val aliasString = if (value.alias.nonEmpty) s" AS ${value.alias.get.name}" else ""
        val expression = ExpressionStringifier.apply().apply(value.expression)
        val input = expression + aliasString
        error(SemanticError.invalidImportingWithAliasOrExpression(input, wth.position))
      }
    }

    def checkDistinct: SemanticCheck = when(wth.distinct) {
      err("DISTINCT")
    }

    def checkOrderBy: SemanticCheck = wth.orderBy.foldSemanticCheck(_ => err("ORDER BY"))

    def checkWhere: SemanticCheck = wth.where.foldSemanticCheck(_ => err("WHERE"))

    def checkSkip: SemanticCheck = wth.skip.foldSemanticCheck(_ => err("SKIP"))

    def checkLimit: SemanticCheck = wth.limit.foldSemanticCheck(_ => err("LIMIT"))

    fromFunctionWithContext { (state, context) =>
      val resultState = wth.returnItems.items.foldSemanticCheck(_.semanticCheck).run(state, context)
      val hasImports = wth.returnItems.includeExisting || wth.returnItems.items.exists {
        item =>
          item.expression
            .endoRewrite(DeclarationsAndDependencies.dependenciesRewriter(resultState.state))
            .dependencies
            .nonEmpty
      }
      val check = when(hasImports) {
        checkReturnItems chain
          checkDistinct chain
          checkWhere chain
          checkOrderBy chain
          checkSkip chain
          checkLimit
      }
      check.run(state, context)
    }
  }

  private def checkStandaloneCall(clauses: Seq[Clause]): SemanticCheck = {
    clauses match {
      case Seq(_: CallClause, where @ With(_, _, _, _, _, _, _, AddedInRewriteProcCall)) =>
        val gql = GqlHelper.getGql42001_42N71_42NAB(
          where.position.offset,
          where.position.line,
          where.position.column
        )
        error(
          gql,
          "Cannot use standalone call with WHERE (instead use: `CALL ... WITH * WHERE ... RETURN *`)",
          where.position
        )
      case Seq(_: GraphSelection, _: CallClause) =>
        // USE clause and standalone procedure call
        success
      case all if all.size > 1 && all.exists(c => c.isInstanceOf[CallClause]) =>
        // Non-standalone procedure call should not allow YIELD *
        clauses.find {
          case uc: CallClause => uc.yieldAll
          case _              => false
        }.map(c =>
          error(SemanticError.invalidYieldStar(c.position))
        )
          .getOrElse(success)
      case _ =>
        success
    }
  }

  private def containsNonCommandClause(clauses: Seq[Clause], onlyAllowReturnIfAddedInRewriter: Boolean): Boolean = {
    clauses.exists(c =>
      !c.isInstanceOf[CommandClause] &&
        !c.isInstanceOf[GraphSelection] &&
        !(c.isInstanceOf[Return] && (
          !onlyAllowReturnIfAddedInRewriter || c.asInstanceOf[Return].returnType == ReturnAddedInRewrite
        )) &&
        !(c.isInstanceOf[With] && (
          c.asInstanceOf[With].withType == ParsedAsYield ||
            c.asInstanceOf[With].withType == AddedInRewriteShowCommands ||
            c.asInstanceOf[With].withType.isInstanceOf[AddedInRewriteGeneral]
        ))
    )
  }

  private def checkComposableNonTransactionCommandsAllowed(context: UnaliasedNotAllowed): SemanticCheck = {
    // Combining commands other than show and terminate transactions are hidden behind a feature flag
    // Same with combining any command with any regular Cypher clause (except their YIELD and RETURN)
    val nonTransactionCommands = getCommandClauses.filter(c => !c.isInstanceOf[TransactionsCommandClause])
    val partOfLargerQuery = context != ImportingWithSubqueryCall

    if (
      getCommandClauses.nonEmpty && (
        partOfLargerQuery || containsNonCommandClause(clauses, onlyAllowReturnIfAddedInRewriter = false)
      )
    ) {
      requireFeatureSupport(
        "Composing `SHOW` and `TERMINATE` commands with regular Cypher",
        SemanticFeature.ComposableCommands,
        position
      )
    } else if (getCommandClauses.size > 1 && nonTransactionCommands.nonEmpty) {
      requireFeatureSupport(
        "Composing commands other than `SHOW TRANSACTIONS` and `TERMINATE TRANSACTIONS`",
        SemanticFeature.ComposableCommands,
        position
      )
    } else {
      success
    }
  }

  private def checkOrderForCommandClauses(clauses: Seq[Clause], context: UnaliasedNotAllowed) = {
    val partOfLargerQuery = context != ImportingWithSubqueryCall
    val containsNonCommand = containsNonCommandClause(clauses, onlyAllowReturnIfAddedInRewriter = true)

    val checkYieldAndReturn = getCommandClauses.size > 1 || {
      // put as a def to not calculate it if not needed
      def exceptions = clauses match {
        // COMMAND CLAUSE [WHERE]
        case Seq(_: CommandClause, withClause: With, returnClause: Return)
          if withClause.withType == AddedInRewriteShowCommands =>
          returnClause.returnType == ReturnAddedInRewrite
        // USE x COMMAND CLAUSE [WHERE]
        case Seq(_: GraphSelection, _: CommandClause, withClause: With, returnClause: Return)
          if withClause.withType == AddedInRewriteShowCommands =>
          returnClause.returnType == ReturnAddedInRewrite
        // COMMAND CLAUSE YIELD [RETURN]
        case Seq(_: CommandClause, withClause: With, returnClause: Return) =>
          withClause.withType == ParsedAsYield || returnClause.returnType == ReturnAddedInRewrite
        // USE x COMMAND CLAUSE YIELD [RETURN]
        case Seq(_: GraphSelection, _: CommandClause, withClause: With, returnClause: Return) =>
          withClause.withType == ParsedAsYield || returnClause.returnType == ReturnAddedInRewrite
        // COMMAND CLAUSE YIELD rewriter-WITH RETURN
        // for when we split out variables from the return to a separate with clause
        case Seq(_: CommandClause, withAsYieldClause: With, rewriterWith: With, returnClause: Return)
          if withAsYieldClause.withType == ParsedAsYield && returnClause.returnType != ReturnAddedInRewrite =>
          rewriterWith.withType.isInstanceOf[AddedInRewriteGeneral]
        // USE x COMMAND CLAUSE YIELD rewriter-WITH RETURN
        // for when we split out variables from the return to a separate with clause
        case Seq(
            _: GraphSelection,
            _: CommandClause,
            withAsYieldClause: With,
            rewriterWith: With,
            returnClause: Return
          )
          if withAsYieldClause.withType == ParsedAsYield && returnClause.returnType != ReturnAddedInRewrite =>
          rewriterWith.withType.isInstanceOf[AddedInRewriteGeneral]
        case _ => false
      }

      getCommandClauses.nonEmpty && (partOfLargerQuery || (containsNonCommand && !exceptions))
    }

    if (checkYieldAndReturn) {
      val missingYield = clauses.sliding(2).foldLeft(Vector.empty[SemanticError]) {
        case (semanticErrors, pair) =>
          val optError = pair match {
            case Seq(command: CommandClause, clause: With) if command.yieldAll =>
              Some(SemanticError.invalidYieldStar(
                command.name,
                clause.position
              ))
            case Seq(_: CommandClause, clause: With) if clause.withType == ParsedAsYield => None
            case Seq(command: CommandClause, _) =>
              Some(SemanticError.missingYield(
                command.name,
                command.position
              ))
            case _ => None
          }
          optError.fold(semanticErrors)(semanticErrors :+ _)
      }

      val missingReturn =
        if (partOfLargerQuery || containsNonCommand)
          checkLastClause(clauses, canOmitReturnClause = false, disallowReturnAddedInRewrite = true)
        else clauses.last match {
          case clause: Return if clause.returnType != ReturnAddedInRewrite => None
          case clause =>
            Some(SemanticError.missingReturn(
              clause.position
            ))
        }

      missingYield ++ missingReturn
    } else Vector.empty[SemanticError]

  }

  private def checkOrder(
    clauses: Seq[Clause],
    canOmitReturnClause: Boolean,
    context: UnaliasedNotAllowed
  ): SemanticCheck = {
    fromFunctionWithContext { (s: SemanticState, c: SemanticCheckContext) =>
      {
        val sequenceErrors = clauses.sliding(2).foldLeft(Vector.empty[SemanticError]) {
          case (semanticErrors, pair) =>
            val optError = pair match {
              case Seq(clause: Return, w: With)
                if w.withType.isInstanceOf[OrderByOrPaginationWithType] =>
                Some(SemanticError.invalidSubclauseOrder(
                  clause.skip.fold(w.skip.fold("SKIP")(_.name))(_.name),
                  clause.position
                ))
              case Seq(clause: Return, _) =>
                Some(SemanticError.invalidPositionOfClause(clause.name, clause.position))
              case Seq(clause: Finish, _) =>
                Some(SemanticError.invalidPositionOfClause(clause.name, clause.position))
              case Seq(_: UpdateClause, _: UpdateClause) =>
                None
              case Seq(_: UpdateClause, _: With) =>
                None
              case Seq(_: UpdateClause, _: Return) =>
                None
              case Seq(_: UpdateClause, _: Finish) =>
                None
              case Seq(update: UpdateClause, clause) if c.cypherVersion == CypherVersion.Cypher5 =>
                Some(SemanticError.withIsRequiredBetween(update.name, clause.name, clause.position))
              case _ =>
                None
            }
            optError.fold(semanticErrors)(semanticErrors :+ _)
        }

        val commandErrors = checkOrderForCommandClauses(clauses, context)

        val concludeError = clauses match {
          // standalone procedure call
          case Seq(_: CallClause)                    => None
          case Seq(_: GraphSelection, _: CallClause) => None

          case Seq() => Some(SemanticError.queryMustConcludeWithClause(this.position))

          // standalone command clause
          case Seq(_: CommandClause) if context == ImportingWithSubqueryCall                    => None
          case Seq(_: GraphSelection, _: CommandClause) if context == ImportingWithSubqueryCall => None

          // otherwise
          case seq => checkLastClause(
              seq,
              canOmitReturnClause,
              disallowReturnAddedInRewrite = getCommandClauses.nonEmpty && context != ImportingWithSubqueryCall
            )
        }

        SemanticCheckResult(s, sequenceErrors ++ concludeError ++ commandErrors)
      }
    }
  }

  // Share end of query check between the CommandClause and general cypher query code path
  @tailrec
  private def checkLastClause(
    clauses: Seq[Clause],
    canOmitReturnClause: Boolean,
    disallowReturnAddedInRewrite: Boolean
  ): Option[SemanticError] = {
    clauses.last match {
      case ret: Return if disallowReturnAddedInRewrite && ret.returnType == ReturnAddedInRewrite =>
        // Return was added in rewrite so check second to last clause
        checkLastClause(clauses.init, canOmitReturnClause, disallowReturnAddedInRewrite)
      case _: UpdateClause | _: Return | _: Finish                                                     => None
      case subquery: SubqueryCall if !subquery.innerQuery.isReturning && subquery.reportParams.isEmpty => None
      case call: CallClause if call.returnVariables.explicitVariables.isEmpty && !call.yieldAll        => None
      case _ if canOmitReturnClause                                                                    => None
      case call: CallClause => Some(SemanticError.queryCannotConcludeWithCall(call.name, call.position))
      case clause           => Some(SemanticError.queryCannotConcludeWithClause(clause.name, clause.position))
    }
  }

  private def checkNoCallInTransactionsAfterWriteClause(clauses: Seq[Clause]): SemanticCheck = {
    case class Acc(precedingWrite: Boolean, errors: Seq[SemanticError])

    val Acc(_, errors) = clauses.foldLeft[Acc](Acc(precedingWrite = false, Seq.empty)) {
      case (Acc(precedingWrite, errors), callInTxs: SubqueryCall) if SubqueryCall.isTransactionalSubquery(callInTxs) =>
        if (precedingWrite) {
          Acc(
            precedingWrite,
            errors :+ SemanticError.invalidUseOfCIT(callInTxs.position)
          )
        } else {
          Acc(precedingWrite, errors)
        }
      case (acc, clause) => Acc(
          acc.precedingWrite || clause.folder.treeExists { case _: UpdateClause => true },
          acc.errors
        )
    }
    errors
  }

  private def checkClauses(
    clauses: Seq[Clause],
    outerScope: Option[Scope],
    context: UnaliasedNotAllowed = ImportingWithSubqueryCall
  ): SemanticCheck = {
    val lastIndex = clauses.size - 1
    clauses.zipWithIndex.foldSemanticCheck {
      case (clause, idx) =>
        clause match {
          case c: Return =>
            checkReturn(c, outerScope, context) chain recordCurrentScope(clause)
          case c: ScopeClauseSubqueryCall =>
            checkHorizon(c, outerScope)
          case c: HorizonClause =>
            checkHorizon(c, outerScope) chain recordCurrentScope(clause)
          case _ =>
            clause.semanticCheck.map { checked =>
              val resultState = clause match {
                case _: UpdateClause if idx == lastIndex =>
                  checked.state.newSiblingScope
                case cc: CallClause
                  if cc.returnVariables.explicitVariables.isEmpty && !cc.yieldAll && idx == lastIndex =>
                  checked.state.newSiblingScope
                case _ =>
                  checked.state
              }
              checked.copy(state = resultState)
            } chain recordCurrentScope(clause)
        }
    }
  }

  private def checkReturn(
    clause: Return,
    outerScope: Option[Scope],
    context: UnaliasedNotAllowed
  ): SemanticCheck = {
    val returnWithContext = clause.copy(context = context)(clause.position)
    for {
      closingResult <- returnWithContext.semanticCheck
      continuationResult <-
        returnWithContext.semanticCheckContinuation(closingResult.state.currentScope.scope, outerScope)
    } yield {
      semantics.SemanticCheckResult(continuationResult.state, closingResult.errors ++ continuationResult.errors)
    }

  }

  private def checkHorizon(clause: HorizonClause, outerScope: Option[Scope]): SemanticCheck = {
    for {
      closingResult <- clause.semanticCheck
      continuationResult <- clause.semanticCheckContinuation(closingResult.state.currentScope.scope, outerScope)
    } yield {
      semantics.SemanticCheckResult(continuationResult.state, closingResult.errors ++ continuationResult.errors)
    }
  }

  private def checkInputDataStream(clauses: Seq[Clause]): SemanticCheck = {
    val idsClauses = clauses.filter(_.isInstanceOf[InputDataStream])

    idsClauses.size match {
      case c if c > 1 =>
        error(SemanticError.internalError(
          this.getClass.getSimpleName,
          "There can be only one INPUT DATA STREAM in a query",
          idsClauses(1).position
        ))
      case c if c == 1 =>
        if (clauses.head.isInstanceOf[InputDataStream]) {
          success
        } else {
          error(SemanticError.internalError(
            this.getClass.getSimpleName,
            "INPUT DATA STREAM must be the first clause in a query",
            idsClauses.head.position
          ))
        }
      case _ => success
    }
  }

  private def checkUsePositionInScopeSubquery(): SemanticCheck = {
    val clauses = partitionedClauses.clausesExceptInitialGraphSelection

    clauses.collect {
      case useGraph: UseGraph => useGraph
    }.foldSemanticCheck { clause =>
      SemanticAnalysisToolingErrorWithGqlInfo.invalidPlacementOfUseClauseError(
        clause.position
      )
    }
  }

  private def checkUsePosition(): SemanticCheck = {
    val clauses = partitionedClauses.clausesExceptImportingWithAndLeadingGraphSelection

    clauses.collect {
      case useGraph: UseGraph => useGraph
    }.foldSemanticCheck { clause =>
      error(SemanticError.invalidPlacementOfUseClauseVerboseLegacyMsg(clause.position))
    }
  }

  private def checkUseForRoutedSystemCommand(): SemanticCheck = SemanticCheck.fromContext { context =>
    // For backward compatibility, SHOW DATABASES is routed to system if it is standalone, but we don't want to allow it to be routed anywhere else,
    // as this could allow it to get routed to a remote alias.
    // Explicit routing should only be allowed from Cypher 25
    partitionedClauses match {
      case SingleQuery.PartitionedClauses(Some(use), _, _, cs)
        if CommandClause.shouldRouteToSystem(cs) && (
          context.cypherVersion == CypherVersion.Cypher5 || use.graphReference.print != SYSTEM_DATABASE_NAME
        ) =>
        SemanticError.useClauseWithAdministrationCommand(use.position)
      case _ => success
    }
  }

  private def warnOnPotentiallyShadowVariables(outer: SemanticState, optional: Boolean): SemanticCheck = {
    (inner: SemanticState) =>
      val outerScopeSymbols: Map[String, Symbol] = outer.currentScope.scope.symbolTable
      val innerScopeSymbols: Map[String, Set[Symbol]] = inner.currentScope.scope.allSymbols

      def isShadowed(s: Symbol): Boolean =
        innerScopeSymbols.contains(s.name) &&
          !innerScopeSymbols(s.name).map(_.definition).contains(s.definition)

      val shadowedSymbols = outerScopeSymbols.collect {
        case (symbolName, symbol) if isShadowed(symbol) =>
          symbolName -> innerScopeSymbols(
            symbolName
          ).find(_.definition != symbol.definition).get.definition.asVariable.position
      }
      val stateWithNotifications = shadowedSymbols.foldLeft(inner) {
        case (state, (varName, pos)) =>
          val clause = if (optional) "OPTIONAL CALL" else "CALL"
          state.addNotification(SubqueryVariableShadowing(pos, clause, varName))
      }

      SemanticCheckResult.success(stateWithNotifications)
  }

  private def errorOnShadowedImportVariables(outer: SemanticState): SemanticCheck = { (inner: SemanticState) =>
    if (getReturns.exists(_.returnType == ReturnAddedInRewrite)) { SemanticCheckResult.success(inner) }
    else {
      val outerScopeSymbols: Map[String, Symbol] = outer.currentScope.scope.symbolTable

      // Finds symbols of children of innerScope
      val childrenTables = inner.currentScope.scope.children.map(_.symbolTable)
      val innerScopeSymbols: Map[String, Set[Symbol]] =
        childrenTables.foldLeft(Map.empty[String, Set[Symbol]]) {
          case (acc0, table) =>
            table.foldLeft(acc0) {
              case (acc, (str, symbol)) if acc.contains(str) =>
                acc.updated(str, acc(str) + symbol)
              case (acc, (str, symbol)) =>
                acc.updated(str, Set(symbol))
            }
        }

      def isShadowed(s: Symbol): Boolean = {
        innerScopeSymbols.contains(s.name) &&
        !innerScopeSymbols(s.name).map(_.definition).forall(_ == s.definition)
      }

      val shadowedSymbols = outerScopeSymbols.collect {
        case (symbolName, symbol) if isShadowed(symbol) =>
          symbolName -> innerScopeSymbols(
            symbolName
          ).find(_.definition != symbol.definition).get.definition.asVariable.position
      }

      val shadowingErrors = shadowedSymbols.map {
        case (varName, pos) =>
          SemanticError.variableShadowingOuterScope(varName, pos)
      }.toSeq

      SemanticCheckResult(inner, shadowingErrors)
    }
  }
}

object SingleQuery {

  /**
   * The clauses making up a single query.
   *
   * @param initialGraphSelection                              A `USE` clause in first position.
   * @param importingWith                                      An importing `WITH` clause in first position, or in second position immediately after [[initialGraphSelection]] when defined.
   * @param subsequentGraphSelection                           A `USE` clause in second position immediately after [[importingWith]]. If this field is defined then [[initialGraphSelection]] cannot be.
   * @param clausesExceptImportingWithAndLeadingGraphSelection All the other clauses afterwards.
   */
  case class PartitionedClauses(
    initialGraphSelection: Option[GraphSelection],
    importingWith: Option[With],
    subsequentGraphSelection: Option[GraphSelection],
    clausesExceptImportingWithAndLeadingGraphSelection: Seq[Clause]
  ) {

    private val leadingGraphSelectionLazy: LazyVal[Option[GraphSelection]] =
      LazyVal(initialGraphSelection.orElse(subsequentGraphSelection))
    def leadingGraphSelection: Option[GraphSelection] = leadingGraphSelectionLazy.value

    private val clausesExceptImportingWithAndInitialGraphSelectionLazy: LazyVal[Seq[Clause]] =
      LazyVal(subsequentGraphSelection.toSeq ++ clausesExceptImportingWithAndLeadingGraphSelection)

    def clausesExceptImportingWithAndInitialGraphSelection: Seq[Clause] =
      clausesExceptImportingWithAndInitialGraphSelectionLazy.value

    private val clausesExceptInitialGraphSelectionLazy: LazyVal[Seq[Clause]] =
      LazyVal(importingWith.toSeq ++ subsequentGraphSelection ++ clausesExceptImportingWithAndLeadingGraphSelection)
    def clausesExceptInitialGraphSelection: Seq[Clause] = clausesExceptInitialGraphSelectionLazy.value

    private val clausesExceptImportingWithLazy: LazyVal[Seq[Clause]] =
      LazyVal(
        initialGraphSelection.toSeq ++ subsequentGraphSelection ++ clausesExceptImportingWithAndLeadingGraphSelection
      )
    def clausesExceptImportingWith: Seq[Clause] = clausesExceptImportingWithLazy.value
  }

  private def partitionClauses(clauses: Seq[Clause]): PartitionedClauses =
    startingWithImportingWith(clauses)
      .orElse(startingWithGraphSelection(clauses))
      .getOrElse(PartitionedClauses(None, None, None, clauses))

  private def startingWithImportingWith(clauses: Seq[Clause]): Option[PartitionedClauses] =
    extractImportingWith(clauses).map { case (importingWith, subsequentClauses) =>
      extractGraphSelection(subsequentClauses).map { case (subsequentGraphSelection, otherClauses) =>
        PartitionedClauses(None, Some(importingWith), Some(subsequentGraphSelection), otherClauses)
      }.getOrElse {
        PartitionedClauses(None, Some(importingWith), None, subsequentClauses)
      }
    }

  private def startingWithGraphSelection(clauses: Seq[Clause]): Option[PartitionedClauses] =
    extractGraphSelection(clauses).map { case (initialGraphSelection, subsequentClauses) =>
      extractImportingWith(subsequentClauses).map { case (importingWith, otherClauses) =>
        PartitionedClauses(Some(initialGraphSelection), Some(importingWith), None, otherClauses)
      }.getOrElse {
        PartitionedClauses(Some(initialGraphSelection), None, None, subsequentClauses)
      }
    }

  private def extractImportingWith(clauses: Seq[Clause]): Option[(With, Seq[Clause])] =
    clauses.headOption.collect {
      case withClause @ With(false, ri, None, None, None, None, None, _: MayBeImportingWithType)
        if ri.items.forall(_.isPassThrough) =>
        (withClause, clauses.tail)
    }

  private def extractGraphSelection(clauses: Seq[Clause]): Option[(UseGraph, Seq[Clause])] =
    clauses.headOption.collect {
      case useGraph: UseGraph => (useGraph, clauses.tail)
    }
}

case object TopLevelBraces extends UnaliasedNotAllowed {
  val name: String = "{ ... }"
  override val msg: String = "{ RETURN ... }"
}

case class TopLevelBraces(
  query: Query,
  use: Option[UseGraph]
)(override val position: InputPosition) extends PartQuery {

  override def singleQuery: SingleQuery = wrapQuery(query, position)

  override def clauses: Seq[Clause] = singleQuery.clauses

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(query.mapEachSingleQuery(f))(position)

  override def getEmittingResultClauses: ResultEmitter = query.getEmittingResultClauses

  private def wrapQuery(innerQuery: Query, position: InputPosition): SingleQuery = {
    val lastClause =
      if (innerQuery.isReturning) {
        val returnVariables = innerQuery.returnVariables
        Return(returnItems =
          ReturnItems(
            if (returnVariables.includeExisting) AdditiveProjection else FreeProjection,
            returnVariables.explicitVariables.map(v =>
              AliasedReturnItem(v.copyId, v.copyId)(position)
            )
          )(position)
        )(position)
      } else Finish()(position)

    SingleQuery(
      Seq(
        ScopeClauseSubqueryCall(
          innerQuery,
          isImportingAll = true,
          Seq.empty,
          None,
          optional = false
        )(position),
        lastClause
      )
    )(position)

  }

  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck = semanticCheck

  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    query.semanticCheckInLocalCallableBodyContext(outer, current, optional) chain recordCurrentScope(this)

  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    query.semanticCheckInSubqueryContext(outer, current, optional) chain recordCurrentScope(this)

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck =
    SemanticCheck.error(SemanticError.invalidUseOfOldCall(TopLevelBraces.name, position))

  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    query.semanticCheckInSubqueryExpressionContext(canOmitReturn, outer, context) chain recordCurrentScope(this)

  override def semanticCheck: SemanticCheck =
    checkNoDefineAndCallInTransactions chain
      query.semanticCheckInContext(TopLevelBraces) chain
      recordCurrentScope(this)

  override def checkImportingWith(optional: Boolean): SemanticCheck = query.checkImportingWith(optional)
  override def invalidImportingWith: Seq[SemanticError] = query.invalidImportingWith
  override def importColumns: Seq[LogicalVariable] = query.importColumns

  override def getImportingWithItems: Seq[ReturnItem] = query.getImportingWithItems

  override def withoutImportingWithAndGraphSelection: Option[TopLevelBraces] =
    query.withoutImportingWithAndGraphSelection.map(q => TopLevelBraces(q, use)(position))

  override def getGraphSelections: Seq[GraphSelection] =
    query.getGraphSelections ++ use

  override def isCorrelated: Boolean = query.isCorrelated

  override def returnVariables: ReturnVariables = query.returnVariables
}

case object UnionContext extends UnaliasedNotAllowed {
  val name: String = "UNION"
  override val msg: String = "UNION"
}

object Union {

  /**
   * This defines a mapping of variables in both parts of the union to variables valid in the scope after the union.
   */
  case class UnionMapping(
    unionVariable: LogicalVariable,
    variableInLhs: LogicalVariable,
    variableInRhs: LogicalVariable
  )

  val errorParam = "UNION subqueries"

}

sealed trait Union extends Query {
  def lhs: Query

  def rhs: PartQuery

  def unionMappings: List[UnionMapping]

  override def getEmittingResultClauses: ResultEmitter =
    ResultEmitter.AndEmitter(Seq(lhs.getEmittingResultClauses, rhs.getEmittingResultClauses))

  override def returnVariables: ReturnVariables = ReturnVariables(
    // If either side of the UNION has a RETURN *,
    // then returnVariables.explicitVariables will not list all variables.
    // Instead, one has to inspect `finalScope` to find all variables.
    lhs.returnVariables.includeExisting || rhs.returnVariables.includeExisting,
    unionMappings.map(_.unionVariable)
  )

  override def getReturns: Seq[Return] = {
    lhs.getReturns ++ rhs.getReturns
  }

  override def importColumns: Seq[LogicalVariable] = lhs.importColumns ++ rhs.importColumns

  override def getImportingWithItems: Seq[ReturnItem] = lhs.getImportingWithItems ++ rhs.getImportingWithItems

  def containsUpdates: Boolean = lhs.containsUpdates || rhs.containsUpdates

  private def checkRecursively(semanticCheck: Query => SemanticCheck): SemanticCheck = {
    def checkSingleQuery(partQuery: PartQuery): SemanticCheck = withScopedState {
      semanticCheck(partQuery) chain
        checkNoInputDataStreamInsideUnionElement(partQuery) chain
        checkNoCallInTransactionInsideUnionElement(partQuery)
    }

    def checkNestedQuery(query: Query): SemanticCheck =
      query match {
        case partQuery: PartQuery       => checkSingleQuery(partQuery)
        case when: ConditionalQueryWhen => withScopedState(semanticCheck(when))
        case union: Union =>
          withScopedState {
            SemanticCheck.nestedCheck(union.checkRecursively(semanticCheck))
          }
        case _: NextStatement => SemanticCheck.error(SemanticError.internalError(
            "invalid union argument",
            "NEXT should never be directly contained within a UNION",
            position
          ))
        case withLocalDefinitions: QueryWithLocalDefinitions => SemanticCheck.error(SemanticError.internalError(
            "invalid union argument",
            "DEFINE should never be directly contained within a UNION",
            position
          ))
      }

    SemanticCheck.fromState(state => {
      checkUnionAggregation chain
        checkNestedQuery(lhs) chain
        SemanticCheck.fromState(newState =>
          SemanticCheck.setState(newState.recordWorkingGraph(state.workingGraph))
        ) chain
        checkSingleQuery(rhs) chain
        defineUnionVariables chain
        SemanticState.recordCurrentScope(this)
    })
  }

  def semanticCheck: SemanticCheck =
    checkNoDefineAndCallInTransactions chain
      checkRecursively(_.semanticCheckInContext(UnionContext))

  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    checkRecursively(
      importValuesFromScope(outer.currentScope.scope) chain
        _.semanticCheckInSubqueryExpressionContext(canOmitReturn, outer, context)
    )

  override def checkImportingWith(optional: Boolean): SemanticCheck =
    SemanticCheck.nestedCheck(lhs.checkImportingWith(optional)) chain
      rhs.checkImportingWith(optional)

  override def invalidImportingWith: Seq[SemanticError] =
    lhs.invalidImportingWith ++ rhs.invalidImportingWith

  override def isCorrelated: Boolean = lhs.isCorrelated || rhs.isCorrelated

  override def isReturning: Boolean = rhs.isReturning // we assume lhs has the same value

  override def endsWithFinish: Boolean = rhs.endsWithFinish || lhs.endsWithFinish

  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck = {
    checkRecursively(innerQuery =>
      importValuesFromScope(outer.currentScope.scope) chain
        innerQuery.semanticCheckInLocalCallableBodyContext(outer, current, optional)
    )
  }

  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck = {
    checkRecursively(innerQuery =>
      importValuesFromScope(outer.currentScope.scope) chain
        innerQuery.semanticCheckInSubqueryContext(outer, current, optional)
    )
  }

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck =
    checkRecursively(_.semanticCheckImportingWithSubQueryContext(outer, optional))

  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck = {
    context match {
      case NextStatement | TopLevelBraces => checkRecursively(innerQuery =>
          mapState(s =>
            s.importValuesFromScope(s.currentScope.parent.fold(Scope.empty)(_.scope))
          ) chain innerQuery.semanticCheckInContext(context)
        )
      case _ => checkRecursively(_.semanticCheckInContext(context))
    }

  }

  private def defineUnionVariables: SemanticCheck = (state: SemanticState) => {
    var result = SemanticCheckResult.success(state.newChildScope)
    val scopeFromLhs = lhs.finalScope(state.scope(lhs).getOrElse(Scope.empty))
    val scopeFromRhs = rhs.finalScope(state.scope(rhs).getOrElse(Scope.empty))

    /**
     * Derived from UnionMapping, but only has the names of the variables in LHS and RHS,
     * since that is also the information we need here.
     */
    case class Mapping(
      unionVariable: LogicalVariable,
      variableInLhsName: String,
      variableInRhsName: String
    )

    val mappings = {
      // We need a Set since otherwise we would declare variables multiple times,
      // e.g. if they are listed, but there is also a *,
      // or if both branches have a *.
      val builder = Set.newBuilder[Mapping]
      unionMappings.foreach { um =>
        builder.addOne(Mapping(um.unionVariable, um.variableInLhs.name, um.variableInRhs.name))
      }
      // If there is a RETURN * in at least one of the UNION branches,
      // we need to find out what extra variables to include here.
      if (lhs.returnVariables.includeExisting) {
        scopeFromLhs.symbolNames.foreach { name =>
          builder.addOne(Mapping(Variable(name)(this.position, Variable.isIsolatedDefault), name, name))
        }
      }
      if (rhs.returnVariables.includeExisting) {
        scopeFromRhs.symbolNames.foreach { name =>
          builder.addOne(Mapping(Variable(name)(this.position, Variable.isIsolatedDefault), name, name))
        }
      }
      builder.result()
    }

    for {
      mapping <- mappings
      symbolFromLhs <- scopeFromLhs.symbol(mapping.variableInLhsName)
      symbolFromRhs <- scopeFromRhs.symbol(mapping.variableInRhsName)
    } yield {
      val unionType = symbolFromLhs.types.union(symbolFromRhs.types)
      result = result.state.declareVariable(mapping.unionVariable, unionType, unionVariable = true) match {
        case Left(err)        => SemanticCheckResult(result.state, err +: result.errors)
        case Right(nextState) => SemanticCheckResult(nextState, result.errors)
      }
    }

    SemanticCheckResult(result.state.popScope, result.errors)
  }

  override def finalScope(scope: Scope): Scope =
    // Union defines all return variables in its own scope using defineUnionVariables
    scope.children.last

  private def checkNoInputDataStreamInsideUnionElement(query: PartQuery): SemanticCheck =
    query
      .clauses
      .collectFirst {
        case inputDataStream: InputDataStream => inputDataStream
      }
      .foldSemanticCheck { inputDataStream =>
        error(SemanticError.internalError(
          this.getClass.getSimpleName,
          "INPUT DATA STREAM is not supported in UNION queries",
          inputDataStream.position
        ))
      }

  private def checkUnionAggregation: SemanticCheck = (lhs, this) match {
    case (_: PartQuery, _)                                        => None
    case (_: UnionAll, _: UnionAll)                               => None
    case (_: UnionDistinct, _: UnionDistinct)                     => None
    case (_: ProjectingUnionAll, _: ProjectingUnionAll)           => None
    case (_: ProjectingUnionDistinct, _: ProjectingUnionDistinct) => None
    case _                                                        => Some(SemanticError.invalidUseOfUnion(position))
  }

  private def checkNoCallInTransactionInsideUnionElement(query: PartQuery): SemanticCheck =
    SubqueryCall
      .findTransactionalSubquery(query)
      .foldSemanticCheck { nestedCallInTransactions =>
        error(SemanticError.invalidUseOfUnionAndCIT(nestedCallInTransactions.position))
      }
}

/**
 * UnmappedUnion classes are directly produced by the parser.
 * When we do namespacing, we need to convert them the [[ProjectingUnion]].
 * ProjectingUnion is never produced by the parser.
 *
 * This is because we capture how variables are projected from the two final scopes of the parts of the union to the scope
 * after the union, before the Namespacer changes the names so that the Variable inside and outside of the union have different names
 * and we would not find them any longer. The Namespacer will still change the name, but since we captured the Variable and not the
 * name, we still have the correct projecting information.
 */
sealed trait UnmappedUnion extends Union {

  // A value instead of a def prevents us from creating new variables every time this is used.
  // This is helpful if the variable is used by reference from the semantic state.
  private var _unionMappings = {
    for {
      lhsCol <- lhs.returnColumns
      rhsCol <- rhs.returnColumns.find(_.name == lhsCol.name)
    } yield {
      // This assumes that lhs.returnColumns and rhs.returnColumns agree
      UnionMapping(Variable(lhsCol.name)(this.position, Variable.isIsolatedDefault), lhsCol, rhsCol)
    }
  }

  override def unionMappings: List[UnionMapping] = _unionMappings

  override def dup(children: Seq[AnyRef]): UnmappedUnion.this.type = {
    val res = super.dup(children)

    val thisLhsCols = lhs.returnColumns
    val thisRhsCols = rhs.returnColumns
    val resLhsCols = res.lhs.returnColumns
    val resRhsCols = res.rhs.returnColumns

    def containTheSameInstances[X <: AnyRef](a: Seq[X], b: Seq[X]): Boolean =
      a.forall(elemA => b.exists(elemB => elemA eq elemB)) && a.size == b.size

    // If we have not rewritten any return column (by reference equality), then we can simply reuse this.unionMappings.
    // This is important because the variables are used by reference from the semantic state.
    if (containTheSameInstances(thisLhsCols, resLhsCols) && containTheSameInstances(thisRhsCols, resRhsCols)) {
      res._unionMappings = this.unionMappings
    }

    res.asInstanceOf[UnmappedUnion.this.type]
  }
}

sealed trait ProjectingUnion extends Union

final case class UnionAll(lhs: Query, rhs: PartQuery)(
  val position: InputPosition
) extends UnmappedUnion {

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(lhs.mapEachSingleQuery(f), f(rhs.singleQuery))(position)

  override def withoutImportingWithAndGraphSelection: Option[UnionAll] = {
    val lhsOpt = lhs.withoutImportingWithAndGraphSelection
    val rhsOpt = rhs.withoutImportingWithAndGraphSelection
    lhsOpt.zip(rhsOpt).map {
      case (lhs, rhs) => UnionAll(lhs, rhs)(position)
    }
  }

  override def getGraphSelections: Seq[GraphSelection] =
    lhs.getGraphSelections ++ rhs.getGraphSelections
}

final case class UnionDistinct(lhs: Query, rhs: PartQuery)(
  val position: InputPosition
) extends UnmappedUnion {

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(lhs.mapEachSingleQuery(f), f(rhs.singleQuery))(position)

  override def withoutImportingWithAndGraphSelection: Option[UnionDistinct] = {
    val lhsOpt = lhs.withoutImportingWithAndGraphSelection
    val rhsOpt = rhs.withoutImportingWithAndGraphSelection
    lhsOpt.zip(rhsOpt).map {
      case (lhs, rhs) => UnionDistinct(lhs, rhs)(position)
    }
  }

  override def getGraphSelections: Seq[GraphSelection] =
    lhs.getGraphSelections ++ rhs.getGraphSelections
}

final case class ProjectingUnionAll(lhs: Query, rhs: PartQuery, unionMappings: List[UnionMapping])(
  val position: InputPosition
) extends ProjectingUnion {

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(lhs.mapEachSingleQuery(f), f(rhs.singleQuery))(position)

  override def withoutImportingWithAndGraphSelection: Option[ProjectingUnionAll] = {
    val lhsOpt = lhs.withoutImportingWithAndGraphSelection
    val rhsOpt = rhs.withoutImportingWithAndGraphSelection
    lhsOpt.zip(rhsOpt).map {
      case (lhs, rhs) => ProjectingUnionAll(lhs, rhs, unionMappings)(position)
    }
  }

  override def getGraphSelections: Seq[GraphSelection] =
    lhs.getGraphSelections ++ rhs.getGraphSelections
}

final case class ProjectingUnionDistinct(lhs: Query, rhs: PartQuery, unionMappings: List[UnionMapping])(
  val position: InputPosition
) extends ProjectingUnion {

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(lhs.mapEachSingleQuery(f), f(rhs.singleQuery))(position)

  override def withoutImportingWithAndGraphSelection: Option[ProjectingUnionDistinct] = {
    val lhsOpt = lhs.withoutImportingWithAndGraphSelection
    val rhsOpt = rhs.withoutImportingWithAndGraphSelection
    lhsOpt.zip(rhsOpt).map {
      case (lhs, rhs) => ProjectingUnionDistinct(lhs, rhs, unionMappings)(position)
    }
  }

  override def getGraphSelections: Seq[GraphSelection] =
    lhs.getGraphSelections ++ rhs.getGraphSelections
}

// Predicate is None for Else branch
case class ConditionalQueryBranch(predicate: Option[Expression], query: PartQuery)(val position: InputPosition)
    extends ASTNode
    with SemanticCheckable with SemanticAnalysisTooling with QueryUtils {

  override def returnVariables: ReturnVariables = query.returnVariables

  override def isReturning: Boolean = query.isReturning

  override def semanticCheck: SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInContext(ConditionalQueryWhen))

  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck = semanticCheck

  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck = {
    importValuesFromScope(outer.currentScope.scope) chain
      semanticCheckAbstract(_.semanticCheckInLocalCallableBodyContext(outer, current, optional))
  }

  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck = {
    importValuesFromScope(outer.currentScope.scope) chain
      semanticCheckAbstract(_.semanticCheckInSubqueryContext(outer, current, optional))
  }

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckImportingWithSubQueryContext(outer, optional))

  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    semanticCheckAbstract(
      importValuesFromScope(outer.currentScope.scope) chain
        _.semanticCheckInSubqueryExpressionContext(canOmitReturn, outer, context)
    )

  private def semanticCheckAbstract(check: QueryUtils => SemanticCheck): SemanticCheck = {
    predicateCheck chain
      check(query) chain
      recordCurrentScope(this)
  }

  private def predicateCheck: SemanticCheck = {
    SemanticExpressionCheck.simple(predicate) chain
      SemanticExpressionCheck.expectType(CTBoolean.covariant, predicate)
  }

  override def finalScope(scope: Scope): Scope =
    if (scope.children.size < 1) Scope.empty else scope.children.last

  def mapEachSingleQuery(f: SingleQuery => SingleQuery): ConditionalQueryBranch =
    copy(query = query.mapEachSingleQuery(f).asInstanceOf[PartQuery])(position)

  def withoutImportingWith: Option[ConditionalQueryBranch] =
    query.withoutImportingWithAndGraphSelection.map(q => ConditionalQueryBranch(predicate, q)(position))

  def getGraphSelections: Seq[GraphSelection] = query.getGraphSelections
}

case object ConditionalQueryWhen extends UnaliasedNotAllowed {
  override val msg: String = "WHEN ... THEN ..."
  val name: String = "conditional queries"
}

case class ConditionalQueryWhen(
  branches: Seq[ConditionalQueryBranch],
  default: Option[ConditionalQueryBranch]
)(val position: InputPosition) extends Query {

  private def allBranches: Seq[ConditionalQueryBranch] = branches ++ default

  override def containsUpdates: Boolean = allBranches.exists(_.query.containsUpdates)

  override def getEmittingResultClauses: ResultEmitter =
    ResultEmitter.OrEmitter(branches.map(_.query.getEmittingResultClauses))

  override def returnVariables: ReturnVariables =
    allBranches.foldLeft(ReturnVariables.empty)((acc, branch) => acc.merge(branch.query.returnVariables))

  override def getReturns: Seq[Return] =
    allBranches.flatMap(_.query.getReturns)

  override def isReturning: Boolean = branches.head.isReturning

  override def endsWithFinish: Boolean = allBranches.exists(_.query.endsWithFinish)

  override def finalScope(scope: Scope): Scope = {
    if (scope.children.size < 1) Scope.empty else scope.children.last
  }

  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInLocalCallableBodyContext(outer, current, optional))

  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInSubqueryContext(outer, current, optional))

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck =
    SemanticCheck.error(SemanticError.invalidUseOfOldCall(msg, position))

  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInSubqueryExpressionContext(canOmitReturn, outer, ConditionalQueryWhen))

  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck =
    semanticCheck

  override def semanticCheck: SemanticCheck =
    checkNoDefineAndCallInTransactions chain
      semanticCheckAbstract(_.semanticCheckInContext(ConditionalQueryWhen))

  private def semanticCheckAbstract(check: QueryUtils => SemanticCheck): SemanticCheck = {
    allBranches.foldSemanticCheck(x => withScopedState(check(x))) chain
      defineReturnScope chain
      recordCurrentScope(this)
  }

  private def defineReturnScope: SemanticCheck = (state: SemanticState) => {
    val result = SemanticCheckResult.success(state.newChildScope)
    val headQuery = branches.head.query
    val headScope = state.scope(headQuery)
    val scope = headQuery.finalScope(headScope.getOrElse(Scope.empty))
    SemanticCheckResult(result.state.importValuesFromScope(scope).popScope, Seq.empty)
  }

  override def checkImportingWith(optional: Boolean): SemanticCheck =
    allBranches.foldSemanticCheck(_.query.checkImportingWith(optional))

  override def invalidImportingWith: Seq[SemanticError] = allBranches.flatMap(_.query.invalidImportingWith)

  override def isCorrelated: Boolean =
    allBranches.exists(_.query.isCorrelated)

  override def importColumns: Seq[LogicalVariable] =
    allBranches.flatMap(_.query.importColumns)

  override def getImportingWithItems: Seq[ReturnItem] =
    allBranches.flatMap(_.query.getImportingWithItems)

  override def withoutImportingWithAndGraphSelection: Option[ConditionalQueryWhen] = {
    if (allBranches.exists(_.withoutImportingWith.isDefined)) {
      Some(ConditionalQueryWhen(
        branches.map(_.withoutImportingWith.get),
        default.map(_.withoutImportingWith.get)
      )(position))
    } else None
  }

  override def getGraphSelections: Seq[GraphSelection] =
    (allBranches.map(_.getGraphSelections) ++ default.map(_.getGraphSelections)).flatten

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(branches.map(_.mapEachSingleQuery(f)), default.map(_.mapEachSingleQuery(f)))(position)
}

case object NextStatement extends UnaliasedNotAllowed {
  val name: String = "NEXT"
  override val msg: String = "RETURN followed by NEXT"
}

case class NextStatement(queries: Seq[Query])(val position: InputPosition) extends Query {

  private def lastQuery = queries.last

  override def returnColumns: List[LogicalVariable] = lastQuery.returnColumns

  override def containsUpdates: Boolean = queries.exists(_.containsUpdates)

  override def getReturns: Seq[Return] = lastQuery.getReturns

  override def endsWithFinish: Boolean = lastQuery.endsWithFinish

  override def checkImportingWith(optional: Boolean): SemanticCheck =
    queries.foldSemanticCheck(_.checkImportingWith(optional))

  override def invalidImportingWith: Seq[SemanticError] = queries.flatMap(_.invalidImportingWith)

  override def isCorrelated: Boolean = queries.exists(_.isCorrelated)

  override def importColumns: Seq[LogicalVariable] = queries.flatMap(_.importColumns)

  override def getImportingWithItems: Seq[ReturnItem] =
    queries.flatMap(_.getImportingWithItems)

  override def withoutImportingWithAndGraphSelection: Option[NextStatement] = {
    if (queries.exists(_.withoutImportingWithAndGraphSelection.isDefined)) {
      Some(NextStatement(queries.map(_.withoutImportingWithAndGraphSelection.get))(position))
    } else None
  }

  override def getGraphSelections: Seq[GraphSelection] =
    queries.flatMap(_.getGraphSelections)

  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    copy(queries.dropRight(1) :+ lastQuery.mapEachSingleQuery(f))(position)

  override def getEmittingResultClauses: ResultEmitter = queries.last.getEmittingResultClauses

  override def returnVariables: ReturnVariables = lastQuery.returnVariables

  override def isReturning: Boolean = lastQuery.isReturning

  override def finalScope(scope: Scope): Scope = lastQuery.finalScope(scope)

  private case class CheckWithPrevious(
    check: Query => SemanticCheck,
    accumulator: SemanticCheck = SemanticCheck.success,
    previous: Option[Query] = None,
    outer: Option[SemanticState]
  ) {

    private def checkNoRedeclarationOfOuter(query: Query): SemanticCheck = {
      val constantSymbols = outer.map(_.currentScope.symbolNames).getOrElse(Set.empty)
      val symbolIntersection =
        query.returnVariables.explicitVariables.filter(v => constantSymbols.contains(v.name))

      val errors = symbolIntersection.map(v => SemanticError.variableAlreadyDeclaredInOuterScope(v.name, v.position))
      when(symbolIntersection.nonEmpty)(SemanticCheck.error(errors))
    }

    private def innerCheck(query: Query): SemanticCheck =
      withScopedState(
        fromState(state => setState(state.recordWorkingGraph(None))) chain
          when(previous.fold(false)(_.isReturning)) {
            previous.map(importValuesFromRecordedFinalScope).getOrElse(SemanticCheck.success)
          } chain
          checkNoRedeclarationOfOuter(query) chain
          query.semanticCheckInContext(NextStatement)
      )

    def checkQuery(query: Query): CheckWithPrevious =
      copy(accumulator = accumulator chain innerCheck(query), previous = Some(query))

  }

  private def noteFinalScope(): SemanticCheck = {
    withScopedState(
      fromState(state =>
        importValuesFromScope(lastQuery.finalScope(state.scope(lastQuery).getOrElse(Scope.empty)))
      )
    )
  }

  private def semanticCheckAbstract(check: Query => SemanticCheck, outer: Option[SemanticState]): SemanticCheck = {
    val trunk = queries.dropRight(1)
    trunk.foldLeft(CheckWithPrevious(check, outer = outer)) {
      case (accCheck, q) => accCheck.checkQuery(q)
    }.accumulator chain
      withScopedState(fromState(s =>
        setState(s.recordWorkingGraph(None)) chain
          when(trunk.last.isReturning) {
            importValuesFromRecordedFinalScope(trunk.last)
          } chain
          check(lastQuery)
      )) chain
      noteFinalScope() chain
      recordCurrentScope(this)
  }

  override def semanticCheck: SemanticCheck =
    checkNoDefineAndCallInTransactions chain
      semanticCheckAbstract(_.semanticCheckInContext(NextStatement), None)

  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInLocalCallableBodyContext(outer, current, optional), Some(outer))

  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInSubqueryContext(outer, current, optional), Some(outer))

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck =
    SemanticCheck.error(SemanticError.invalidUseOfOldCall(NextStatement.name, position))

  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInSubqueryExpressionContext(canOmitReturn, outer, context), Some(outer))

  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInContext(context), None)

}

case object QueryWithLocalDefinitions extends UnaliasedNotAllowed {
  val name: String = "local callable definitions"
  override val msg: String = "DEFINE"
}

case class QueryWithLocalDefinitions(
  definitions: Seq[LocalCallableDefinition],
  query: Query
)(val position: InputPosition)
    extends Query {

  override def containsUpdates: Boolean = query.containsUpdates

  /**
   * All Return clauses contained within this statement.
   */
  override def getReturns: Seq[Return] = query.getReturns

  /**
   * True iff this query part ends with a finish clause.
   */
  override def endsWithFinish: Boolean = query.endsWithFinish

  /**
   * Check this query part if it starts with an importing WITH
   */
  override def checkImportingWith(optional: Boolean): SemanticCheck = query.checkImportingWith(optional)

  /**
   * True if this query part starts with an importing WITH (has incoming arguments)
   */
  override def isCorrelated: Boolean = query.isCorrelated

  /**
   * Returns names of variables imported using importing WITH
   */
  override def importColumns: Seq[LogicalVariable] = query.importColumns

  override def getImportingWithItems: Seq[ReturnItem] = query.getImportingWithItems

  /**
   * Returns the query stripped from importing WITH responsible for top-level importing.
   */
  override def withoutImportingWithAndGraphSelection: Option[Query] =
    query.withoutImportingWithAndGraphSelection.map(q => QueryWithLocalDefinitions(definitions, q)(position))

  override def getGraphSelections: Seq[GraphSelection] = query.getGraphSelections

  /**
   * Return a copy of this query where the mapping function f is applied
   * to each returning single query, regardless if this a single query or a union query.
   */
  override def mapEachSingleQuery(f: SingleQuery => SingleQuery): Query =
    query.mapEachSingleQuery(f)

  override def getEmittingResultClauses: ResultEmitter = query.getEmittingResultClauses

  /**
   * All variables that are explicitly listed to be returned from this statement.
   * This also includes the information whether all other potentially existing variables in scope are also returned.
   */
  override def returnVariables: ReturnVariables = query.returnVariables

  /**
   * True iff this query part ends with a return clause.
   */
  override def isReturning: Boolean = query.isReturning

  /**
   * Given the root scope for this query part,
   * looks up the final scope after the last clause
   */
  override def finalScope(scope: Scope): Scope = query.finalScope(scope)

  private def semanticCheckAbstract(check: Query => SemanticCheck): SemanticCheck = {
    requireFeatureSupport(
      "The DEFINE keyword",
      SemanticFeature.LocalCallables,
      position
    ) ifOkChain
      SemanticCheck.fromState { outer =>
        // Detached base: no parent chain => no variables visible
        val defsBase =
          outer.copy(currentScope = Scope.empty.location).newChildScope
        // (newChildScope is optional here, but it avoids “Top” issues if any code wants to insert siblings.)

        SemanticCheck.setState(defsBase) chain
          definitions.foldSemanticCheck(_.semanticCheck) chain
          SemanticCheck.fromState { defsState =>
            val outerWithGraphs = updateRecordedGraphs(outer, defsState)
            val mergedOuter =
              outerWithGraphs.copy(
                recordedScopes = outerWithGraphs.recordedScopes ++ defsState.recordedScopes,
                typeTable = outerWithGraphs.typeTable ++ defsState.typeTable,
                notifications = outerWithGraphs.notifications ++ defsState.notifications
              )
            SemanticCheck.setState(mergedOuter)
          }
      } ifOkChain
      check(query)
  }

  override def semanticCheck: SemanticCheck =
    checkNoDefineAndCallInTransactions chain
      semanticCheckAbstract(_.semanticCheckInContext(QueryWithLocalDefinitions))

  /**
   * Semantic check for when this `Query` is enclosed in outer context
   */
  override def semanticCheckInContext(context: UnaliasedNotAllowed): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInContext(context))

  /**
   * Semantic check for when this `Query` is the body of local callable, and imports
   * local callable parameters as variables from the `outer` scope
   */
  override def semanticCheckInLocalCallableBodyContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInLocalCallableBodyContext(outer, current, optional))

  /**
   * Semantic check for when this `Query` is in a subquery, and might import
   * variables from the `outer` scope
   */
  override def semanticCheckInSubqueryContext(
    outer: SemanticState,
    current: SemanticState,
    optional: Boolean
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInSubqueryContext(outer, current, optional))

  override def semanticCheckImportingWithSubQueryContext(outer: SemanticState, optional: Boolean): SemanticCheck =
    requireFeatureSupport(
      "The DEFINE keyword",
      SemanticFeature.LocalCallables,
      position
    ) ifOkChain
      SemanticCheck.error(SemanticError.invalidUseOfOldCall(QueryWithLocalDefinitions.name, position))

  /**
   * Exists and Count can omit the Return Statement
   * Count still requires it for Distinct Unions as in this case the count
   * changes based on which rows are distinct vs not
   */
  override def semanticCheckInSubqueryExpressionContext(
    canOmitReturn: Boolean,
    outer: SemanticState,
    context: UnaliasedNotAllowed
  ): SemanticCheck =
    semanticCheckAbstract(_.semanticCheckInSubqueryExpressionContext(canOmitReturn, outer, context))

  override def invalidImportingWith: Seq[SemanticError] = query.invalidImportingWith
}
