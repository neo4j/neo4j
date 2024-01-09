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
package org.neo4j.cypher.internal.cst.factory.neo4j.ast

import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener

trait StatementBuilder extends CypherParserListener {

  final override def exitStatements(
    ctx: CypherParser.StatementsContext
  ): Unit = {}

  final override def exitStatement(
    ctx: CypherParser.StatementContext
  ): Unit = {}

  final override def exitSingleQueryOrCommand(
    ctx: CypherParser.SingleQueryOrCommandContext
  ): Unit = {}

  final override def exitSingleQueryOrCommandWithUseClause(
    ctx: CypherParser.SingleQueryOrCommandWithUseClauseContext
  ): Unit = {}

  final override def exitPeriodicCommitQueryHintFailure(
    ctx: CypherParser.PeriodicCommitQueryHintFailureContext
  ): Unit = {}

  final override def exitRegularQuery(
    ctx: CypherParser.RegularQueryContext
  ): Unit = {}

  final override def exitUnion(
    ctx: CypherParser.UnionContext
  ): Unit = {}

  final override def exitSingleQuery(
    ctx: CypherParser.SingleQueryContext
  ): Unit = {}

  final override def exitSingleQueryWithUseClause(
    ctx: CypherParser.SingleQueryWithUseClauseContext
  ): Unit = {}

  final override def exitClause(
    ctx: CypherParser.ClauseContext
  ): Unit = {}

  final override def exitUseClause(
    ctx: CypherParser.UseClauseContext
  ): Unit = {}

  final override def exitGraphReference(
    ctx: CypherParser.GraphReferenceContext
  ): Unit = {}

  final override def exitReturnClause(
    ctx: CypherParser.ReturnClauseContext
  ): Unit = {}

  final override def exitReturnBody(
    ctx: CypherParser.ReturnBodyContext
  ): Unit = {}

  final override def exitReturnItem(
    ctx: CypherParser.ReturnItemContext
  ): Unit = {}

  final override def exitReturnItems(
    ctx: CypherParser.ReturnItemsContext
  ): Unit = {}

  final override def exitOrderItem(
    ctx: CypherParser.OrderItemContext
  ): Unit = {}

  final override def exitSkip(
    ctx: CypherParser.SkipContext
  ): Unit = {}

  final override def exitLimit(
    ctx: CypherParser.LimitContext
  ): Unit = {}

  final override def exitWhereClause(
    ctx: CypherParser.WhereClauseContext
  ): Unit = {}

  final override def exitWithClause(
    ctx: CypherParser.WithClauseContext
  ): Unit = {}

  final override def exitCreateClause(
    ctx: CypherParser.CreateClauseContext
  ): Unit = {}

  final override def exitSetClause(
    ctx: CypherParser.SetClauseContext
  ): Unit = {}

  final override def exitSetItem(
    ctx: CypherParser.SetItemContext
  ): Unit = {}

  final override def exitRemoveClause(
    ctx: CypherParser.RemoveClauseContext
  ): Unit = {}

  final override def exitRemoveItem(
    ctx: CypherParser.RemoveItemContext
  ): Unit = {}

  final override def exitDeleteClause(
    ctx: CypherParser.DeleteClauseContext
  ): Unit = {}

  final override def exitMatchClause(
    ctx: CypherParser.MatchClauseContext
  ): Unit = {}

  final override def exitMatchMode(
    ctx: CypherParser.MatchModeContext
  ): Unit = {}

  final override def exitHints(
    ctx: CypherParser.HintsContext
  ): Unit = {}

  final override def exitIndexHintBody(
    ctx: CypherParser.IndexHintBodyContext
  ): Unit = {}

  final override def exitMergeClause(
    ctx: CypherParser.MergeClauseContext
  ): Unit = {}

  final override def exitUnwindClause(
    ctx: CypherParser.UnwindClauseContext
  ): Unit = {}

  final override def exitCallClause(
    ctx: CypherParser.CallClauseContext
  ): Unit = {}

  final override def exitLoadCSVClause(
    ctx: CypherParser.LoadCSVClauseContext
  ): Unit = {}

  final override def exitForeachClause(
    ctx: CypherParser.ForeachClauseContext
  ): Unit = {}

  final override def exitSubqueryClause(
    ctx: CypherParser.SubqueryClauseContext
  ): Unit = {}

}
