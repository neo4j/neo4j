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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.util.InputPosition

/* Test base for combining listing and terminating commands */
class CombineCommandsParserTestBase extends AdministrationAndSchemaCommandParserTestBase {

  override protected def ignorePrettifier: Boolean = true

  protected def showTx(
    ids: CommandClauseNames,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowTransactionsClause(ids, where.map(_._1), yieldItems, yieldAll, yieldWith, returnCypher5Types = false)

  protected def terminateTx(
    ids: CommandClauseNames,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.TerminateTransactionsClause(ids, yieldItems, yieldAll, yieldWith, where.map(_._2))

  protected def showSetting(
    ids: CommandClauseNames,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowSettingsClause(ids, where.map(_._1), yieldItems, yieldAll, yieldWith)

  protected def showFunction(
    functionType: ast.ShowFunctionType,
    executable: Option[ast.ExecutableBy],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowFunctionsClause(functionType, executable, where.map(_._1), yieldItems, yieldAll, yieldWith)

  protected def showProcedure(
    executable: Option[ast.ExecutableBy],
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowProceduresClause(executable, where.map(_._1), yieldItems, yieldAll, yieldWith)

  protected def showConstraint(
    constraintType: ast.ShowConstraintType,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowConstraintsClause(
      constraintType,
      where.map(_._1),
      yieldItems,
      yieldAll,
      yieldWith,
      returnCypher5Columns = false
    )

  protected def showIndex(
    indexType: ast.ShowIndexType,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowIndexesClause(indexType, where.map(_._1), yieldItems, yieldAll, yieldWith)

  protected def showCurrentGraphType(
    asGraph: Boolean,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowCurrentGraphTypeClause(asGraph, where.map(_._1), yieldItems, yieldAll, yieldWith)

  protected def showDatabase(
    dbScope: DatabaseScope,
    where: Option[(ast.Where, InputPosition)],
    yieldAll: Boolean,
    yieldItems: List[ast.CommandResultItem],
    yieldWith: Option[ast.With]
  ): InputPosition => ast.CommandClause =
    ast.ShowDatabasesClause(dbScope, where.map(_._1), yieldItems, yieldAll, yieldWith, cypher5ColumnsOnly = false)

}
