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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

trait ShowCommand extends Parser
                  with Base
                  with GraphSelection
                  with CommandHelper {

  def ShowSchemaCommand: Rule1[ast.Query] = rule(ShowIndexes | ShowConstraints | ShowProcedures | ShowFunctions | ShowTransactions | TerminateTransactions)

  private def briefVerboseOutput: Rule1[Boolean] = rule("type of show output") {
    keyword("VERBOSE") ~~ optional(keyword("OUTPUT")) ~~~> (_ => true) |
    keyword("BRIEF") ~~ optional(keyword("OUTPUT")) ~~~> (_ => false)
  }

  private def ExecutableByClause: Rule1[ast.ExecutableBy] = rule("EXECUTABLE BY") {
    keyword("EXECUTABLE BY CURRENT USER") ~~~> (_ => ast.CurrentUser) |
    keyword("EXECUTABLE BY") ~~ SymbolicNameString ~~>> (name => _ => ast.User(name)) |
    keyword("EXECUTABLE") ~~~> (_ => ast.CurrentUser)
  }

  // SHOW INDEXES

  private def ShowIndexes: Rule1[ast.Query] = rule("SHOW INDEXES") {
    UseGraph ~~ ShowIndexesClauses ~~>> ((use, show) => pos => ast.Query(None, ast.SingleQuery(use +: show)(pos))(pos)) |
    ShowIndexesClauses ~~>> (show => pos => ast.Query(None, ast.SingleQuery(show)(pos))(pos))
  }

  private def ShowIndexesClauses: Rule1[Seq[ast.Clause]] = rule("SHOW INDEXES YIELD / WHERE / RETURN") {
    keyword("SHOW") ~~ OldIndexType ~~ IndexKeyword ~~ briefVerboseOutput  ~~>>
      ((indexType, verbose) => pos => Seq(ast.ShowIndexesClause(indexType, !verbose, verbose, None, hasYield = false)(pos))) |
    keyword("SHOW") ~~ IndexType ~~ IndexKeyword ~~ ShowCommandClauses  ~~>>
      ((indexType, clauses) => pos => clauses match {
        case Right(where) => Seq(ast.ShowIndexesClause(indexType, brief = false, verbose = false, Some(where), hasYield = false)(pos))
        case Left((y, Some(r))) => Seq(ast.ShowIndexesClause(indexType, brief = false, verbose = false, None, hasYield = true)(pos), y, r)
        case Left((y, None)) => Seq(ast.ShowIndexesClause(indexType, brief = false, verbose = false, None, hasYield = true)(pos), y)
      }) |
    keyword("SHOW") ~~ IndexType ~~ IndexKeyword ~~>>
      (indexType => pos => Seq(ast.ShowIndexesClause(indexType, brief = false, verbose = false, None, hasYield = false)(pos)))
  }

  private def OldIndexType: Rule1[ast.ShowIndexType] = rule("type of indexes") {
    keyword("BTREE") ~~~> (_ => ast.BtreeIndexes) |
    optional(keyword("ALL")) ~~~> (_ => ast.AllIndexes)
  }

  private def IndexType: Rule1[ast.ShowIndexType] = rule("type of indexes") {
    keyword("BTREE") ~~~> (_ => ast.BtreeIndexes) |
    keyword("RANGE") ~~~> (_ => ast.RangeIndexes) |
    keyword("FULLTEXT") ~~~> (_ => ast.FulltextIndexes) |
    keyword("TEXT") ~~~> (_ => ast.TextIndexes) |
    keyword("POINT") ~~~> (_ => ast.PointIndexes) |
    keyword("LOOKUP") ~~~> (_ => ast.LookupIndexes) |
    optional(keyword("ALL")) ~~~> (_ => ast.AllIndexes)
  }

  // SHOW CONSTRAINTS

  private def ShowConstraints: Rule1[ast.Query] = rule("SHOW CONSTRAINTS") {
    UseGraph ~~ ShowConstraintsClauses ~~>> ((use, show) => pos => ast.Query(None, ast.SingleQuery(use +: show)(pos))(pos)) |
    ShowConstraintsClauses ~~>> (show => pos => ast.Query(None, ast.SingleQuery(show)(pos))(pos))
  }

  private def ShowConstraintsClauses: Rule1[Seq[ast.Clause]] = rule("SHOW CONSTRAINTS YIELD / WHERE / RETURN") {
    keyword("SHOW") ~~ OldConstraintType ~~ ConstraintKeyword ~~ briefVerboseOutput ~~>>
      ((constraintType, verbose) => pos => Seq(ast.ShowConstraintsClause(constraintType, !verbose, verbose, None, hasYield = false)(pos))) |
    keyword("SHOW") ~~ NewConstraintType ~~ ConstraintKeyword ~~ ShowCommandClauses ~~>>
      ((constraintType, clauses) => pos => clauses match {
        case Right(where) => Seq(ast.ShowConstraintsClause(constraintType, brief = false, verbose = false, Some(where), hasYield = false)(pos))
        case Left((y, Some(r))) => Seq(ast.ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = true)(pos), y, r)
        case Left((y, None)) => Seq(ast.ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = true)(pos), y)
      }) |
    keyword("SHOW") ~~ MixedConstraintType ~~ ConstraintKeyword ~~>>
      (constraintType => pos => Seq(ast.ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(pos)))
  }

  private def OldConstraintType: Rule1[ast.ShowConstraintType] = rule("old type of constraints") {
    keyword("UNIQUE") ~~~> (_ => ast.UniqueConstraints) |
    keyword("NODE KEY") ~~~> (_ => ast.NodeKeyConstraints) |
    keyword("NODE") ~~ OldExistencePart ~~> (ecs => ast.NodeExistsConstraints(ecs)) |
    keyword("RELATIONSHIP") ~~ OldExistencePart ~~> (ecs => ast.RelExistsConstraints(ecs)) |
    OldExistencePart ~~> (ecs => ast.ExistsConstraints(ecs)) |
    optional(keyword("ALL")) ~~~> (_ => ast.AllConstraints)
  }

  private def MixedConstraintType: Rule1[ast.ShowConstraintType] = rule("type of constraints") {
    keyword("UNIQUE") ~~~> (_ => ast.UniqueConstraints) |
    keyword("NODE KEY") ~~~> (_ => ast.NodeKeyConstraints) |
    keyword("NODE") ~~ MixedExistencePart ~~> (ecs => ast.NodeExistsConstraints(ecs)) |
    keyword("RELATIONSHIP") ~~ MixedExistencePart ~~> (ecs => ast.RelExistsConstraints(ecs)) |
    keyword("REL") ~~ NewExistencePart ~~> (_ => ast.RelExistsConstraints(ast.ValidSyntax)) |
    MixedExistencePart ~~> (ecs => ast.ExistsConstraints(ecs)) |
    optional(keyword("ALL")) ~~~> (_ => ast.AllConstraints)
  }

  private def NewConstraintType: Rule1[ast.ShowConstraintType] = rule("type of constraints") {
    keyword("UNIQUE") ~~~> (_ => ast.UniqueConstraints) |
    keyword("NODE KEY") ~~~> (_ => ast.NodeKeyConstraints) |
    keyword("NODE") ~~ NewExistencePart ~~> (ecs => ast.NodeExistsConstraints(ecs)) |
    keyword("RELATIONSHIP") ~~ NewExistencePart ~~> (ecs => ast.RelExistsConstraints(ecs)) |
    keyword("REL") ~~ NewExistencePart ~~> (_ => ast.RelExistsConstraints(ast.ValidSyntax)) |
    NewExistencePart ~~> (ecs => ast.ExistsConstraints(ecs)) |
    optional(keyword("ALL")) ~~~> (_ => ast.AllConstraints)
  }

  private def OldExistencePart: Rule1[ast.ExistenceConstraintSyntax] = rule {
    keyword("EXISTS") ~~~> (_ => ast.RemovedSyntax) |
    keyword("EXIST") ~~~> (_ => ast.ValidSyntax)
  }

  private def MixedExistencePart: Rule1[ast.ExistenceConstraintSyntax] = rule {
    keyword("PROPERTY EXISTENCE") ~~~> (_ => ast.ValidSyntax) |
    keyword("PROPERTY EXIST") ~~~> (_ => ast.ValidSyntax) |
    keyword("EXISTENCE") ~~~> (_ => ast.ValidSyntax) |
    keyword("EXISTS") ~~~> (_ => ast.RemovedSyntax) |
    keyword("EXIST") ~~~> (_ => ast.ValidSyntax)
  }

  private def NewExistencePart: Rule1[ast.ExistenceConstraintSyntax] = rule {
    keyword("PROPERTY EXISTENCE") ~~~> (_ => ast.ValidSyntax) |
    keyword("PROPERTY EXIST") ~~~> (_ => ast.ValidSyntax) |
    keyword("EXISTENCE") ~~~> (_ => ast.ValidSyntax) |
    keyword("EXIST") ~~~> (_ => ast.ValidSyntax)
  }

  // SHOW PROCEDURES

  private def ShowProcedures: Rule1[ast.Query] = rule("SHOW PROCEDURES") {
    UseGraph ~~ ShowProceduresClauses ~~>> ((use, show) => pos => ast.Query(None, ast.SingleQuery(use +: show)(pos))(pos)) |
    ShowProceduresClauses ~~>> (show => pos => ast.Query(None, ast.SingleQuery(show)(pos))(pos))
  }

  private def ShowProceduresClauses: Rule1[Seq[ast.Clause]] = rule("SHOW PROCEDURES YIELD / WHERE / RETURN") {
    val showProceduresClausesStart = keyword("SHOW") ~~ ProcedureKeyword
    showProceduresClausesStart ~~ ExecutableByClause ~~ ShowCommandClauses ~~>> ((executable, clauses) => pos => showProcClauses(Some(executable), clauses, pos)) |
    showProceduresClausesStart ~~ ExecutableByClause ~~>> (executable => pos => Seq(ast.ShowProceduresClause(Some(executable), None, hasYield = false)(pos))) |
    showProceduresClausesStart ~~ ShowCommandClauses ~~>> (clauses => pos => showProcClauses(None, clauses, pos)) |
    showProceduresClausesStart ~~~> (pos => Seq(ast.ShowProceduresClause(None, None, hasYield = false)(pos)))
  }

  private def showProcClauses(executableBy: Option[ast.ExecutableBy], clauses: Either[(ast.Yield, Option[ast.Return]), ast.Where], pos: InputPosition): Seq[ast.Clause] = clauses match {
    case Right(where)       => Seq(ast.ShowProceduresClause(executableBy, Some(where), hasYield = false)(pos))
    case Left((y, Some(r))) => Seq(ast.ShowProceduresClause(executableBy, None, hasYield = true)(pos), y, r)
    case Left((y, None))    => Seq(ast.ShowProceduresClause(executableBy, None, hasYield = true)(pos), y)
  }

  // SHOW FUNCTIONS

  private def ShowFunctions: Rule1[ast.Query] = rule("SHOW FUNCTIONS") {
    UseGraph ~~ ShowFunctionsClauses ~~>> ((use, show) => pos => ast.Query(None, ast.SingleQuery(use +: show)(pos))(pos)) |
    ShowFunctionsClauses ~~>> (show => pos => ast.Query(None, ast.SingleQuery(show)(pos))(pos))
  }

  private def ShowFunctionsClauses: Rule1[Seq[ast.Clause]] = rule("SHOW FUNCTIONS YIELD / WHERE / RETURN") {
    val showFunctionClausesStart = keyword("SHOW") ~~ FunctionType ~~ FunctionKeyword
    showFunctionClausesStart ~~ ExecutableByClause ~~ ShowCommandClauses ~~>>
      ((functionType, executable, clauses) => pos => showFuncClauses(functionType, Some(executable), clauses, pos)) |
    showFunctionClausesStart ~~ ExecutableByClause ~~>>
      ((functionType, executable) => pos => Seq(ast.ShowFunctionsClause(functionType, Some(executable), None, hasYield = false)(pos))) |
    showFunctionClausesStart ~~ ShowCommandClauses ~~>>
      ((functionType, clauses) => pos => showFuncClauses(functionType, None, clauses, pos)) |
    showFunctionClausesStart ~~>>
      (functionType => pos => Seq(ast.ShowFunctionsClause(functionType, None, None, hasYield = false)(pos)))
  }

  private def FunctionType: Rule1[ast.ShowFunctionType] = rule("type of functions") {
    keyword("BUILT IN") ~~~> (_ => ast.BuiltInFunctions) |
    keyword("USER DEFINED") ~~~> (_ => ast.UserDefinedFunctions) |
    optional(keyword("ALL")) ~~~> (_ => ast.AllFunctions)
  }

  private def showFuncClauses(functionType: ast.ShowFunctionType,
                              executableBy: Option[ast.ExecutableBy],
                              clauses: Either[(ast.Yield, Option[ast.Return]), ast.Where],
                              pos: InputPosition): Seq[ast.Clause] = clauses match {
    case Right(where)       => Seq(ast.ShowFunctionsClause(functionType, executableBy, Some(where), hasYield = false)(pos))
    case Left((y, Some(r))) => Seq(ast.ShowFunctionsClause(functionType, executableBy, None, hasYield = true)(pos), y, r)
    case Left((y, None))    => Seq(ast.ShowFunctionsClause(functionType, executableBy, None, hasYield = true)(pos), y)
  }

  // Transaction commands

  private def TransactionIdStringList: Rule1[List[String]] = rule("a list of string transaction ids") {
    //noinspection LanguageFeature
    (oneOrMore(WS ~~ StringLiteral ~~ WS, separator = ",") memoMismatches).suppressSubnodes ~~>>(strings => _ => strings.map(_.value))
  }

  private def TransactionIds: Rule1[Either[List[String], Parameter]] = rule("transaction ids, either a parameter or a list of strings") {
    TransactionIdStringList ~~> (ids => Left(ids)) |
    Parameter ~~> (p => Right(p))
  }

  // SHOW TRANSACTIONS

  private def ShowTransactions: Rule1[ast.Query] = rule("SHOW TRANSACTIONS") {
    UseGraph ~~ ShowTransactionsClauses ~~>> ((use, show) => pos => ast.Query(None, ast.SingleQuery(use +: show)(pos))(pos)) |
    ShowTransactionsClauses ~~>> (show => pos => ast.Query(None, ast.SingleQuery(show)(pos))(pos))
  }

  private def ShowTransactionsClauses: Rule1[Seq[ast.Clause]] = rule("SHOW TRANSACTIONS YIELD / WHERE / RETURN") {
    val showTransactionsClausesStart = keyword("SHOW") ~~ TransactionKeyword
    group(showTransactionsClausesStart ~~ TransactionIds ~~ ShowCommandClauses) ~~>>
      ((ids, clauses) => pos => showTransactionClauses(ids, clauses, pos)) |
    group(showTransactionsClausesStart ~~ ShowCommandClauses) ~~>>
      (clauses => pos => showTransactionClauses(Left(List.empty), clauses, pos)) |
    group(showTransactionsClausesStart ~~ TransactionIds) ~~>>
      (ids => pos => Seq(ast.ShowTransactionsClause(ids, None, hasYield = false)(pos))) |
    showTransactionsClausesStart ~~~>
      (pos => Seq(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = false)(pos)))
  }

  private def showTransactionClauses(ids: Either[List[String], Parameter],
                                     clauses: Either[(ast.Yield, Option[ast.Return]), ast.Where],
                                     pos: InputPosition): Seq[ast.Clause] = clauses match {
    case Right(where)       => Seq(ast.ShowTransactionsClause(ids, Some(where), hasYield = false)(pos))
    case Left((y, Some(r))) => Seq(ast.ShowTransactionsClause(ids, None, hasYield = true)(pos), y, r)
    case Left((y, None))    => Seq(ast.ShowTransactionsClause(ids, None, hasYield = true)(pos), y)
  }

  // TERMINATE TRANSACTIONS

  private def TerminateTransactions: Rule1[ast.Query] = rule("TERMINATE TRANSACTIONS") {
    UseGraph ~~ TerminateTransactionsClauses ~~>> ((use, show) => pos => ast.Query(None, ast.SingleQuery(use +: show)(pos))(pos)) |
    TerminateTransactionsClauses ~~>> (show => pos => ast.Query(None, ast.SingleQuery(show)(pos))(pos))
  }

  private def TerminateTransactionsClauses: Rule1[Seq[ast.Clause]] = rule("TERMINATE TRANSACTIONS") {
    val terminateTransactionsClausesStart = keyword("TERMINATE") ~~ TransactionKeyword
    terminateTransactionsClausesStart ~~ TransactionIds ~~>>
      (ids => pos => Seq(ast.TerminateTransactionsClause(ids)(pos))) |
    terminateTransactionsClausesStart ~~~>
      (pos => Seq(ast.TerminateTransactionsClause(Left(List.empty))(pos)))
  }
}
