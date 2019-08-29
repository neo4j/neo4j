/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.ast.Union.UnionMapping
import org.neo4j.cypher.internal.v4_0.ast.semantics.{Scope, SemanticAnalysisTooling, SemanticCheckResult, SemanticCheckable, SemanticState, _}
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalVariable, Variable}
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition}

import scala.annotation.tailrec

case class Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart)(val position: InputPosition)
  extends Statement with SemanticAnalysisTooling {

  override def returnColumns: List[LogicalVariable] = part.returnColumns

  def finalScope(scope: Scope): Scope = part.finalScope(scope)

  override def semanticCheck =
    part.semanticCheck chain
    periodicCommitHint.semanticCheck chain
    when(periodicCommitHint.nonEmpty && !part.containsUpdates) {
      SemanticError("Cannot use periodic commit in a non-updating query", periodicCommitHint.get.position)
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
}

case class SingleQuery(clauses: Seq[Clause])(val position: InputPosition) extends QueryPart {
  assert(clauses.nonEmpty)

  override def containsUpdates =
    clauses.exists {
      case call: CallClause => !call.containsNoUpdates
      case _: UpdateClause => true
      case _               => false
    }

  override def returnColumns: List[LogicalVariable] = clauses.last.returnColumns

  override def semanticCheck =
    checkStandaloneCall chain
    checkOrder chain
    checkClauses chain
    checkIndexHints chain
    checkInputDataStream

  private def checkIndexHints: SemanticCheck = s => {
    val hints = clauses.collect { case m: Match => m.hints }.flatten
    val hasStartClause = clauses.exists(_.isInstanceOf[Start])
    if (hints.nonEmpty && hasStartClause) {
      SemanticCheckResult.error(s, SemanticError("Cannot use planner hints with start clause", hints.head.position))
    } else {
      SemanticCheckResult.success(s)
    }
  }

  private def checkStandaloneCall: SemanticCheck = s => {
    clauses match {
      case Seq(call: UnresolvedCall, where: With) =>
        SemanticCheckResult.error(s, SemanticError("Cannot use standalone call with WHERE (instead use: `CALL ... WITH * WHERE ... RETURN *`)", where.position))
      case _ =>
        SemanticCheckResult.success(s)
    }
  }

  private def checkOrder: SemanticCheck = s => {
    val (lastPair, errors) = clauses.sliding(2).foldLeft(Seq.empty[Clause] -> Vector.empty[SemanticError]) {
      case ((_, semanticErrors), pair) =>
        val optError = pair match {
          case Seq(_: With, _: Start) =>
            None
          case Seq(clause, start: Start) =>
            Some(SemanticError(s"WITH is required between ${clause.name} and ${start.name}", clause.position, start.position))
          case Seq(match1: Match, match2: Match) if match1.optional && !match2.optional =>
            Some(SemanticError(s"${match2.name} cannot follow OPTIONAL ${match1.name} (perhaps use a WITH clause between them)", match2.position, match1.position))
          case Seq(clause: ReturnGraph, _) =>
            Some(SemanticError(s"${clause.name} can only be used at the end of the query", clause.position))
          case Seq(clause: Return, _) =>
            Some(SemanticError(s"${clause.name} can only be used at the end of the query", clause.position))
          case Seq(_: UpdateClause, _: UpdateClause) =>
            None
          case Seq(_: UpdateClause, _: With) =>
            None
          case Seq(_: UpdateClause, _: Return) =>
            None
          case Seq(update: UpdateClause, clause) =>
            Some(SemanticError(s"WITH is required between ${update.name} and ${clause.name}", clause.position, update.position))
          case _ =>
            None
        }
        (pair, optError.fold(semanticErrors)(semanticErrors :+ _))
    }

    val lastError = {
      val clause = lastPair.last
      if (SingleQuery.canConcludeWith(clause, clauses.size))
        None
      else
        Some(SemanticError(s"Query cannot conclude with ${clause.name} (must be RETURN or an update clause)", clause.position))
    }

    semantics.SemanticCheckResult(s, errors ++ lastError)
  }

  private def checkClauses: SemanticCheck = s => {
    val result = clauses.zipWithIndex.foldLeft(SemanticCheckResult.success(s.newChildScope)) {
      case (lastResult, (clause, idx)) =>
        val next = clause match {
          case w: With if idx == 0 && lastResult.state.features(SemanticFeature.WithInitialQuerySignature) =>
            checkHorizon(w, lastResult.state.recogniseInitialWith, lastResult.errors)
          case c: HorizonClause =>
            checkHorizon(c, lastResult.state.clearInitialWith, lastResult.errors)
          case _ =>
            val result = clause.semanticCheck(lastResult.state.clearInitialWith)
            SemanticCheckResult(result.state, lastResult.errors ++ result.errors)
        }

        next.copy(state = next.state.recordCurrentScope(clause))
    }
    SemanticCheckResult(result.state.popScope.recordCurrentScope(this), result.errors)
  }

  private def checkHorizon(clause: HorizonClause, state: SemanticState, prevErrors: Seq[SemanticErrorDef]) = {
    val closingResult = clause.semanticCheck(state)
    val nextState = closingResult.state.newSiblingScope
    val continuationResult = clause.semanticCheckContinuation(closingResult.state.currentScope.scope)(nextState)
    semantics.SemanticCheckResult(continuationResult.state, prevErrors ++ closingResult.errors ++ continuationResult.errors)
  }

  private def checkInputDataStream: SemanticCheck = (state: SemanticState) => {
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

  def finalScope(scope: Scope): Scope =
    scope.children.last
}

object SingleQuery {
  def canConcludeWith(clause: Clause, numClauses: Int): Boolean = clause match {
    case _: UpdateClause | _: Return | _: ReturnGraph => true
    case _: CallClause if numClauses == 1             => true
    case _                                            => false
  }
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

  def semanticCheck: SemanticCheck =
    checkUnionAggregation chain
      withScopedState(part.semanticCheck) chain
      withScopedState(query.semanticCheck) chain
      checkColumnNamesAgree chain
      checkInputDataStream chain
      defineUnionVariables chain
      SemanticState.recordCurrentScope(this)

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

  def finalScope(scope: Scope): Scope =
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
 *    and we would not find them any longer. The namespacer will still change the name, but since we captured the Variable and not the
 *    name, we still have the correct projecting information.
 * b) We need to disable `checkColumnNamesAgree` for ProjectingUnion, because the names will actually not agree any more after the namespacing.
 *    This is not a problem though, since we would have failed earlier if the names did not agree originally.
 */
trait UnmappedUnion extends Union {

  override def unionMappings: List[UnionMapping] = {
    for {
      partCol <- part.returnColumns
      queryCol <- query.returnColumns.find(_.name == partCol.name)
    } yield {
      // This assumes that part.returnColumns and query.returnColumns agree
      UnionMapping(Variable(partCol.name)(this.position), partCol, queryCol)
    }
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

trait ProjectingUnion extends Union {
  // If we have a ProjectingUnion we have already checked this before and now they have been rewritten to actually not match.
  override def checkColumnNamesAgree: SemanticCheck = SemanticCheckResult.success
}

final case class UnionAll(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends UnmappedUnion
final case class UnionDistinct(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends UnmappedUnion

final case class ProjectingUnionAll(part: QueryPart, query: SingleQuery, unionMappings: List[UnionMapping])(val position: InputPosition) extends ProjectingUnion
final case class ProjectingUnionDistinct(part: QueryPart, query: SingleQuery, unionMappings: List[UnionMapping])(val position: InputPosition) extends ProjectingUnion
