/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.Symbol
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.SubqueryVariableShadowing

import scala.annotation.tailrec

case class Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart)(val position: InputPosition)
  extends Statement with SemanticAnalysisTooling {

  override def returnColumns: List[LogicalVariable] = part.returnColumns

  def finalScope(scope: Scope): Scope = part.finalScope(scope)

  override def semanticCheck: SemanticCheck =
    part.semanticCheck chain
    periodicCommitHint.semanticCheck chain
    disallowNonUpdatingInPeriodicCommit chain
    disallowCallInTransactionsInPeriodicCommit

  override def containsUpdates: Boolean = part.containsUpdates

  private def disallowNonUpdatingInPeriodicCommit: SemanticCheck =
    when(periodicCommitHint.nonEmpty && !part.containsUpdates) {
      SemanticError("Cannot use periodic commit in a non-updating query", periodicCommitHint.get.position)
    }

  private def disallowCallInTransactionsInPeriodicCommit: SemanticCheck =
    when(periodicCommitHint.nonEmpty) {
      SubqueryCall.findTransactionalSubquery(part) match {
        case Some(subqueryCall) => SemanticError("CALL { ... } IN TRANSACTIONS in a PERIODIC COMMIT query is not supported", subqueryCall.position)
        case None               => success
      }
    }
}

sealed trait QueryPart extends ASTNode with SemanticCheckable {
  def containsUpdates: Boolean
  def returnColumns: List[LogicalVariable]

  /**
   * Given the root scope for this query part,
   * looks up the final scope after the last clause
   */
  def finalScope(scope: Scope): Scope

  /**
   * Check this query part if it start with an importing WITH
   */
  def checkImportingWith: SemanticCheck

  /**
   * Semantic check for when this `QueryPart` is in a subquery, and might import
   * variables from the `outer` scope
   */
  def semanticCheckInSubqueryContext(outer: SemanticState): SemanticCheck

  /**
   * True if this query part starts with an importing WITH (has incoming arguments)
   */
  def isCorrelated: Boolean

  /**
   * True iff this query part ends with a return clause.
   */
  def isYielding: Boolean
}

case class SingleQuery(clauses: Seq[Clause])(val position: InputPosition) extends QueryPart with SemanticAnalysisTooling {
  assert(clauses.nonEmpty)

  override def containsUpdates: Boolean =
    clauses.exists {
      case sub: SubqueryCall => sub.part.containsUpdates
      case call: CallClause  => !call.containsNoUpdates
      case _: UpdateClause  => true
      case _                => false
    }

  override def returnColumns: List[LogicalVariable] = clauses.last.returnColumns

  override def isCorrelated: Boolean = importWith.isDefined

  override def isYielding: Boolean = clauses.last match {
    case _: Return => true
    case _ => false
  }

  def importColumns: Seq[String] = importWith match {
    case Some(w) => w.returnItems.items.map(_.name)
    case _       => Seq.empty
  }

  def importWith: Option[With] = {
    def hasImportFormat(w: With) = w match {
      case With(false, ri, None, None, None, None) =>
        ri.items.forall(_.isPassThrough)
      case _ =>
        false
    }

    clausesExceptLeadingFrom
      .headOption.collect { case w: With if hasImportFormat(w) => w }
  }

  private def leadingNonImportWith: Option[With] = {
    if (importWith.isDefined)
      None
    else
      clausesExceptLeadingFrom.headOption.collect { case wth: With => wth}
  }

  private def leadingGraphSelection: Option[GraphSelection] =
    clauses.headOption.collect { case s: GraphSelection => s }

  def clausesExceptLeadingImportWith: Seq[Clause] = {
    // Find the first occurrence of the importWith clause and split the sequence by it
    val (beforeImportWith, afterIncludingImportWith) = clauses.span(clause => !importWith.contains(clause))

    // Remove the importWith clause and re-assemble the sequence
    beforeImportWith ++ afterIncludingImportWith.drop(1)
  }


  private def clausesExceptLeadingFrom: Seq[Clause] =
    clauses.filterNot(leadingGraphSelection.contains)

  private def clausesExceptLeadingFromAndImportWith: Seq[Clause] = {
    clausesExceptLeadingImportWith.filterNot(leadingGraphSelection.contains)
  }

  private def semanticCheckAbstract(clauses: Seq[Clause], clauseCheck: Seq[Clause] => SemanticCheck): SemanticCheck =
    checkStandaloneCall(clauses) chain
      withScopedState(clauseCheck(clauses)) chain
      checkOrder(clauses) chain
      checkNoCallInTransactionsAfterWriteClause(clauses) chain
      checkIndexHints(clauses) chain
      checkInputDataStream(clauses) chain
      recordCurrentScope(this)

  override def semanticCheck: SemanticCheck =
    semanticCheckAbstract(clauses, checkClauses(_, None))

  override def checkImportingWith: SemanticCheck = importWith.foldSemanticCheck(_.semanticCheck)

  override def semanticCheckInSubqueryContext(outer: SemanticState): SemanticCheck = {
    def importVariables: SemanticCheck =
      importWith.foldSemanticCheck(wth =>
        wth.semanticCheckContinuation(outer.currentScope.scope) chain
        recordCurrentScope(wth)
      )

    checkIllegalImportWith chain
    checkLeadingFrom(outer) chain
    semanticCheckAbstract(
      clausesExceptLeadingFromAndImportWith,
      importVariables chain checkClauses(_, Some(outer.currentScope.scope))
    ) chain
    checkShadowedVariables(outer)
  }

  private def checkLeadingFrom(outer: SemanticState): SemanticCheck =
    leadingGraphSelection match {
      case Some(from) => withState(outer)(from.semanticCheck)
      case None       => success
    }

  private def checkIllegalImportWith: SemanticCheck = leadingNonImportWith.foldSemanticCheck { wth =>
    def err(msg: String): SemanticCheck =
      error(s"Importing WITH should consist only of simple references to outside variables. $msg.", wth.position)

    def checkReturnItems: SemanticCheck = {
      val hasAliases = wth.returnItems.items.exists(!_.isPassThrough)
      when (hasAliases) { err("Aliasing or expressions are not supported") }
    }

    def checkDistinct: SemanticCheck = when (wth.distinct) { err("DISTINCT is not allowed") }
    def checkOrderBy: SemanticCheck = wth.orderBy.foldSemanticCheck(_ => err("ORDER BY is not allowed"))
    def checkWhere: SemanticCheck = wth.where.foldSemanticCheck(_ => err("WHERE is not allowed"))
    def checkSkip: SemanticCheck = wth.skip.foldSemanticCheck(_ => err("SKIP is not allowed"))
    def checkLimit: SemanticCheck = wth.limit.foldSemanticCheck(_ => err("LIMIT is not allowed"))

    val hasImports = wth.returnItems.includeExisting || wth.returnItems.items.exists(_.expression.dependencies.nonEmpty)
    when (hasImports) {
      checkReturnItems chain
        checkDistinct chain
        checkWhere chain
        checkOrderBy chain
        checkSkip chain
        checkLimit
    }
  }

  private def checkIndexHints(clauses: Seq[Clause]): SemanticCheck = s => {
    val hints = clauses.collect { case m: Match => m.hints }.flatten
    val hasStartClause = clauses.exists(_.isInstanceOf[Start])
    if (hints.nonEmpty && hasStartClause) {
      SemanticCheckResult.error(s, SemanticError("Cannot use planner hints with start clause", hints.head.position))
    } else {
      SemanticCheckResult.success(s)
    }
  }

  private def checkStandaloneCall(clauses: Seq[Clause]): SemanticCheck = s => {
    clauses match {
      case Seq(_: UnresolvedCall, where: With) =>
        SemanticCheckResult.error(s, SemanticError("Cannot use standalone call with WHERE (instead use: `CALL ... WITH * WHERE ... RETURN *`)", where.position))
      case Seq(_: GraphSelection, _: UnresolvedCall) =>
        // USE clause and standalone procedure call
        SemanticCheckResult.success(s)
      case all if all.size > 1 && all.exists(c => c.isInstanceOf[UnresolvedCall]) =>
        // Non-standalone procedure call should not allow YIELD *
        clauses.find {
          case uc: UnresolvedCall => uc.yieldAll
          case _ => false
        }.map(c => SemanticCheckResult.error(s, SemanticError("Cannot use `YIELD *` outside standalone call", c.position)))
         .getOrElse(SemanticCheckResult.success(s))
      case _ =>
        SemanticCheckResult.success(s)
    }
  }

  private def checkOrder(clauses: Seq[Clause]): SemanticCheck = s => {
    val sequenceErrors = clauses.sliding(2).foldLeft(Vector.empty[SemanticError]) {
      case (semanticErrors, pair) =>
        val optError = pair match {
          case Seq(_: With, _: Start) =>
            None
          case Seq(clause, start: Start) =>
            Some(SemanticError(s"WITH is required between ${clause.name} and ${start.name}", clause.position))
          case Seq(match1: Match, match2: Match) if match1.optional && !match2.optional =>
            Some(SemanticError(s"${match2.name} cannot follow OPTIONAL ${match1.name} (perhaps use a WITH clause between them)", match2.position))
          case Seq(clause: Return, _) =>
            Some(SemanticError(s"${clause.name} can only be used at the end of the query", clause.position))
          case Seq(_: UpdateClause, _: UpdateClause) =>
            None
          case Seq(_: UpdateClause, _: With) =>
            None
          case Seq(_: UpdateClause, _: Return) =>
            None
          case Seq(update: UpdateClause, clause) =>
            Some(SemanticError(s"WITH is required between ${update.name} and ${clause.name}", clause.position))
          case _ =>
            None
        }
        optError.fold(semanticErrors)(semanticErrors :+ _)
    }

    val validLastClauses = "a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD"

    val concludeError = clauses match {
      // standalone procedure call
      case Seq(_: CallClause)                    => None
      case Seq(_: GraphSelection, _: CallClause) => None

      case Seq() =>
        Some(SemanticError(s"Query must conclude with $validLastClauses", this.position))

      // otherwise
      case seq => seq.last match {
        case _: UpdateClause | _: Return | _: CommandClause                   => None
        case subquery: SubqueryCall if !subquery.part.isYielding              => None
        case call: CallClause if call.returnColumns.isEmpty && !call.yieldAll => None
        case call: CallClause                                                 =>
          Some(SemanticError(s"Query cannot conclude with ${call.name} together with YIELD", call.position))
        case clause                                                           =>
          Some(SemanticError(s"Query cannot conclude with ${clause.name} (must be $validLastClauses)", clause.position))
      }
    }

    semantics.SemanticCheckResult(s, sequenceErrors ++ concludeError)
  }

  private def checkNoCallInTransactionsAfterWriteClause(clauses: Seq[Clause]): SemanticCheck = {
    case class Acc(precedingWrite: Boolean, errors: Seq[SemanticError])

    val Acc(_, errors) = clauses.foldLeft[Acc](Acc(precedingWrite = false, Seq.empty)) {
      case (Acc(precedingWrite, errors), callInTxs:SubqueryCall) if SubqueryCall.isTransactionalSubquery(callInTxs) =>
        if (precedingWrite) {
          Acc(precedingWrite, errors :+ SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", callInTxs.position))
        } else {
          Acc(precedingWrite, errors)
        }
      case (acc, clause) => Acc(
        acc.precedingWrite || clause.treeExists { case _: UpdateClause => true },
        acc.errors
      )
    }
    errors
  }

  private def checkClauses(clauses: Seq[Clause], outerScope: Option[Scope]): SemanticCheck = initialState => {
    val lastIndex = clauses.size - 1
    clauses.zipWithIndex.foldLeft(SemanticCheckResult.success(initialState)) {
      case (lastResult, (clause, idx)) =>
        val next = clause match {
          case w: With if idx == 0 && lastResult.state.features(SemanticFeature.WithInitialQuerySignature) =>
            checkHorizon(w, lastResult.state.recogniseInitialWith, None, lastResult.errors)
          case c: HorizonClause =>
            checkHorizon(c, lastResult.state.clearInitialWith, outerScope, lastResult.errors)
          case _ =>
            val checked = clause.semanticCheck(lastResult.state.clearInitialWith)
            val errors = lastResult.errors ++ checked.errors
            val resultState = clause match {
              case _: UpdateClause if idx == lastIndex =>
                checked.state.newSiblingScope
              case cc: CallClause if cc.returnColumns.isEmpty && !cc.yieldAll && idx == lastIndex =>
                checked.state.newSiblingScope
              case _ =>
                checked.state
            }
            SemanticCheckResult(resultState, errors)
        }

        next.copy(state = next.state.recordCurrentScope(clause))
    }
  }

  private def checkHorizon(clause: HorizonClause, state: SemanticState, outerScope: Option[Scope], prevErrors: Seq[SemanticErrorDef]) = {
    val closingResult = clause.semanticCheck(state)
    val continuationResult = clause.semanticCheckContinuation(closingResult.state.currentScope.scope, outerScope)(closingResult.state)
    semantics.SemanticCheckResult(continuationResult.state, prevErrors ++ closingResult.errors ++ continuationResult.errors)
  }

  private def checkInputDataStream(clauses: Seq[Clause]): SemanticCheck = (state: SemanticState) => {
    val idsClauses = clauses.filter(_.isInstanceOf[InputDataStream])

    idsClauses.size match {
      case c if c > 1 => SemanticCheckResult.error(state, SemanticError("There can be only one INPUT DATA STREAM in a query", idsClauses(1).position))
      case c if c == 1 =>
        if (clauses.head.isInstanceOf[InputDataStream]) {
          SemanticCheckResult.success(state)
        } else {
          SemanticCheckResult.error(state, SemanticError("INPUT DATA STREAM must be the first clause in a query", idsClauses.head.position))
        }
      case _ => SemanticCheckResult.success(state)
    }
  }

  private def checkShadowedVariables(outer: SemanticState): SemanticCheck = { inner =>
    val outerScopeSymbols: Map[String, Symbol] = outer.currentScope.scope.symbolTable
    val innerScopeSymbols: Map[String, Set[Symbol]] = inner.currentScope.scope.allSymbols

    def isShadowed(s: Symbol): Boolean =
      innerScopeSymbols.contains(s.name) &&
        !innerScopeSymbols(s.name).map(_.definition).contains(s.definition)

    val shadowedSymbols = outerScopeSymbols.collect {
      case (name, symbol) if isShadowed(symbol)  =>
        name -> innerScopeSymbols(name).find(_.definition != symbol.definition).get.definition.asVariable.position
    }
    val stateWithNotifications = shadowedSymbols.foldLeft(inner) {
      case (state, (varName, pos)) =>
        state.addNotification(SubqueryVariableShadowing(pos, varName))
    }

    success(stateWithNotifications)
  }

  override def finalScope(scope: Scope): Scope =
    scope.children.last
}

object Union {

  /**
   * This defines a mapping of variables in both parts of the union to variables valid in the scope after the union.
   */
  case class UnionMapping(unionVariable: LogicalVariable, variableInPart: LogicalVariable, variableInQuery: LogicalVariable)
}

sealed trait Union extends QueryPart with SemanticAnalysisTooling {
  def part: QueryPart
  def query: SingleQuery

  def unionMappings: List[UnionMapping]

  def returnColumns: List[LogicalVariable] = unionMappings.map(_.unionVariable)

  def containsUpdates: Boolean = part.containsUpdates || query.containsUpdates

  def semanticCheckAbstract(partCheck: QueryPart => SemanticCheck, queryCheck: SingleQuery => SemanticCheck): SemanticCheck =
    checkUnionAggregation chain
      withScopedState(partCheck(part)) chain
      withScopedState(queryCheck(query)) chain
      checkColumnNamesAgree chain
      defineUnionVariables chain
      checkInputDataStream chain
      checkNoCallInTransactionInsideUnion chain
      SemanticState.recordCurrentScope(this)

  def semanticCheck: SemanticCheck =
    semanticCheckAbstract(
      part => part.semanticCheck,
      query => query.semanticCheck
    )

  override def checkImportingWith: SemanticCheck =
    part.checkImportingWith chain
      query.checkImportingWith

  override def isCorrelated: Boolean = query.isCorrelated || part.isCorrelated

  override def isYielding: Boolean = query.isYielding // we assume part has the same value

  def semanticCheckInSubqueryContext(outer: SemanticState): SemanticCheck =
    semanticCheckAbstract(
      part => part.semanticCheckInSubqueryContext(outer),
      query => query.semanticCheckInSubqueryContext(outer)
    )

  private def defineUnionVariables: SemanticCheck = (state: SemanticState) => {
    var result = SemanticCheckResult.success(state)
    val scopeFromPart = part.finalScope(state.scope(part).get)
    val scopeFromQuery = query.finalScope(state.scope(query).get)
    for {
      unionMapping <- unionMappings
      symbolFromPart <- scopeFromPart.symbol(unionMapping.variableInPart.name)
      symbolFromQuery <- scopeFromQuery.symbol(unionMapping.variableInQuery.name)
    } yield {
      val unionType = symbolFromPart.types.union(symbolFromQuery.types)
      result = result.state.declareVariable(unionMapping.unionVariable, unionType) match {
        case Left(err) => SemanticCheckResult(result.state, err +: result.errors)
        case Right(nextState) => SemanticCheckResult(nextState, result.errors)
      }
    }
    result
  }

  override def finalScope(scope: Scope): Scope =
    // Union defines all return variables in its own scope using defineUnionVariables
    scope

  // Check that columns names agree between both parts of the union
  def checkColumnNamesAgree: SemanticCheck

  private def checkInputDataStream: SemanticCheck = (state: SemanticState) => {

    def checkSingleQuery(query : SingleQuery, state: SemanticState) = {
      val idsClause = query.clauses.find(_.isInstanceOf[InputDataStream])
      if (idsClause.isEmpty) {
        SemanticCheckResult.success(state)
      } else {
        SemanticCheckResult.error(state, SemanticError("INPUT DATA STREAM is not supported in UNION queries", idsClause.get.position))
      }
    }

    val partResult = part match {
      case q : SingleQuery => checkSingleQuery(q, state)
      case _ => SemanticCheckResult.success(state)
    }

    val queryResult = checkSingleQuery(query, state)
    SemanticCheckResult(state, partResult.errors ++ queryResult.errors)
  }

  private def checkUnionAggregation: SemanticCheck = (part, this) match {
    case (_: SingleQuery, _) => None
    case (_: UnionAll, _: UnionAll) => None
    case (_: UnionDistinct, _: UnionDistinct) => None
    case (_: ProjectingUnionAll, _: ProjectingUnionAll) => None
    case (_: ProjectingUnionDistinct, _: ProjectingUnionDistinct) => None
    case _ => Some(SemanticError("Invalid combination of UNION and UNION ALL", position))
  }

  private def checkNoCallInTransactionInsideUnion: SemanticCheck = {
    val nestedCallInTransactions = Seq(part, query).flatMap{qp => SubqueryCall.findTransactionalSubquery(qp)}

    nestedCallInTransactions.foldSemanticCheck { nestedCallInTransactions =>
      error("CALL { ... } IN TRANSACTIONS in a UNION is not supported", nestedCallInTransactions.position)
    }
  }

  def unionedQueries: Seq[SingleQuery] = unionedQueries(Vector.empty)
  @tailrec
  private def unionedQueries(accum: Seq[SingleQuery]): Seq[SingleQuery] = part match {
    case q: SingleQuery => accum :+ query :+ q
    case u: Union       => u.unionedQueries(accum :+ query)
  }
}

/**
 * UnmappedUnion classes are directly produced by the parser.
 * When we do namespacing, we need to convert them the [[ProjectingUnion]].
 * ProjectingUnion is never produced by the parser.
 *
 * This has two reasons:
 * a) We capture how variables are projected from the two final scopes of the parts of the union to the scope
 *    after the union, before the Namespacer changes the names so that the Variable inside and outside of the union have different names
 *    and we would not find them any longer. The Namespacer will still change the name, but since we captured the Variable and not the
 *    name, we still have the correct projecting information.
 * b) We need to disable `checkColumnNamesAgree` for ProjectingUnion, because the names will actually not agree any more after the namespacing.
 *    This is not a problem though, since we would have failed earlier if the names did not agree originally.
 */
sealed trait UnmappedUnion extends Union {

  // A value instead of a def prevents us from creating new variables every time this is used.
  // This is helpful if the variable is used by reference from the semantic state.
  private var _unionMappings = {
    for {
      partCol <- part.returnColumns
      queryCol <- query.returnColumns.find(_.name == partCol.name)
    } yield {
      // This assumes that part.returnColumns and query.returnColumns agree
      UnionMapping(Variable(partCol.name)(this.position), partCol, queryCol)
    }
  }

  override def unionMappings: List[UnionMapping] = _unionMappings

  override def dup(children: Seq[AnyRef]): UnmappedUnion.this.type = {
    val res = super.dup(children)

    val thisPartCols = part.returnColumns
    val thisQueryCols = query.returnColumns
    val resPartCols = res.part.returnColumns
    val resQueryCols = res.query.returnColumns

    def containTheSameInstances[X <: AnyRef](a: Seq[X], b: Seq[X]): Boolean = a.forall(elemA => b.exists(elemB => elemA eq elemB)) && a.size == b.size

    // If we have not rewritten any return column (by reference equality), then we can simply reuse this.unionMappings.
    // This is important because the variables are used by reference from the semantic state.
    if (containTheSameInstances(thisPartCols, resPartCols) && containTheSameInstances(thisQueryCols, resQueryCols)) {
      res._unionMappings = this.unionMappings
    }

    res
  }

  override def checkColumnNamesAgree: SemanticCheck = (state: SemanticState) => {
    val myScope: Scope = state.currentScope.scope

    val partScope = part.finalScope(myScope.children.head)
    val queryScope = query.finalScope(myScope.children.last)
    val errors = if (partScope.symbolNames == queryScope.symbolNames) {
      Seq.empty
    } else {
      Seq(SemanticError("All sub queries in an UNION must have the same column names", position))
    }
    semantics.SemanticCheckResult(state, errors)
  }
}

sealed trait ProjectingUnion extends Union {
  // If we have a ProjectingUnion we have already checked this before and now they have been rewritten to actually not match.
  override def checkColumnNamesAgree: SemanticCheck = SemanticCheckResult.success
}

final case class UnionAll(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends UnmappedUnion
final case class UnionDistinct(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends UnmappedUnion

final case class ProjectingUnionAll(part: QueryPart, query: SingleQuery, unionMappings: List[UnionMapping])(val position: InputPosition) extends ProjectingUnion
final case class ProjectingUnionDistinct(part: QueryPart, query: SingleQuery, unionMappings: List[UnionMapping])(val position: InputPosition) extends ProjectingUnion
