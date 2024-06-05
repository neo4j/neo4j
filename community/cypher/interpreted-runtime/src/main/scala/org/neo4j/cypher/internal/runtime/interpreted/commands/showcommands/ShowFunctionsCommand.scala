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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowFunctionType
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.aggregatingColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.argumentDescriptionColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.categoryColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.deprecatedByColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.descriptionColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.isBuiltInColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.isDeprecatedColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.nameColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.returnDescriptionColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.rolesBoostedExecutionColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.rolesExecutionColumn
import org.neo4j.cypher.internal.ast.ShowFunctionsClause.signatureColumn
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.CypherScope
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.FunctionInformation.InputInformation
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala

// SHOW [ALL | BUILT IN | USER DEFINED] FUNCTION[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
case class ShowFunctionsCommand(
  functionType: ShowFunctionType,
  executableBy: Option[ExecutableBy],
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  isCommunity: Boolean
) extends Command(columns, yieldColumns) {

  private val rolesColumnRequested =
    requestedColumnsNames.contains(rolesExecutionColumn) || requestedColumnsNames.contains(rolesBoostedExecutionColumn)

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    lazy val systemGraph = state.query.systemGraph

    val privileges =
      if (!isCommunity && (rolesColumnRequested || executableBy.isDefined))
        ShowProcFuncCommandHelper.getPrivileges(systemGraph, "FUNCTION")
      else ShowProcFuncCommandHelper.Privileges(List.empty, List.empty, List.empty, List.empty)

    val txContext = state.query.transactionalContext
    val securityContext = txContext.securityContext
    val (userRoles, allRoles, alwaysExecutable) =
      if (!isCommunity) {
        val (userRoles, alwaysExecutable) = ShowProcFuncCommandHelper.getRolesForExecutableByUser(
          securityContext,
          txContext.securityAuthorizationHandler,
          systemGraph,
          executableBy,
          "SHOW FUNCTIONS"
        )
        val allRoles =
          if (functionType != UserDefinedFunctions && (rolesColumnRequested || executableBy.isDefined))
            getAllRoles(systemGraph) // We will need roles column for built-in functions
          else Set.empty[String]

        (userRoles, allRoles, alwaysExecutable)
      } else {
        (Set.empty[String], Set.empty[String], true)
      }
    val allowShowRoles: Boolean =
      if (!isCommunity && rolesColumnRequested)
        securityContext.allowsAdminAction(
          new AdminActionOnResource(SHOW_ROLE, DatabaseScope.ALL, Segment.ALL)
        ).allowsAccess()
      else
        false

    // gets you all functions provided by the query language
    val languageFunctionsInfo = functionType match {
      case UserDefinedFunctions =>
        List.empty // Will anyway filter out all built-in functions and all of these are built-in
      case _ => state.query.providedLanguageFunctions.map(f => FunctionInfo(f)).toList
    }

    // gets you all non-aggregating functions that are registered in the db (incl. those from libs like apoc)
    val loadedFunctions = txContext.procedures.functionGetAll(CypherScope.CYPHER_5).iterator.asScala

    // filters out functions annotated with @Internal and gets the FunctionInfo
    val loadedFunctionsInfo =
      loadedFunctions.filter(f => !f.internal).map(f => FunctionInfo(f, aggregating = false)).toList

    // gets you all aggregation functions that are registered in the db (incl. those from libs like apoc)
    val loadedAggregationFunctions =
      txContext.procedures.aggregationFunctionGetAll(CypherScope.CYPHER_5).iterator.asScala

    // filters out functions annotated with @Internal and gets the FunctionInfo
    val loadedAggregationFunctionsInfo =
      loadedAggregationFunctions.filter(f => !f.internal).map(f => FunctionInfo(f, aggregating = true)).toList

    val allFunctions = languageFunctionsInfo ++ loadedFunctionsInfo ++ loadedAggregationFunctionsInfo
    val filteredFunctions = functionType match {
      case AllFunctions         => allFunctions
      case BuiltInFunctions     => allFunctions.filter(f => f.isBuiltIn)
      case UserDefinedFunctions => allFunctions.filter(f => !f.isBuiltIn)
    }
    val sortedFunctions = filteredFunctions.sortBy(a => a.name)

    val rows = sortedFunctions.map { func =>
      val (executeRoles, boostedExecuteRoles, allowedExecute) =
        if (!isCommunity && (rolesColumnRequested || executableBy.isDefined)) {
          if (func.isBuiltIn) (allRoles, Set.empty[String], userRoles.nonEmpty)
          else
            ShowProcFuncCommandHelper.roles(
              func.name,
              isAdmin = false,
              privileges,
              userRoles
            ) // There is no admin functions (only applicable to procedures)
        } else (Set.empty[String], Set.empty[String], isCommunity)

      executableBy match {
        case Some(_) =>
          getResultMap(func, alwaysExecutable || allowedExecute, executeRoles, boostedExecuteRoles, allowShowRoles)
        case None => getResultMap(func, executeRoles, boostedExecuteRoles, allowShowRoles)
      }
    }.filter(m => m.nonEmpty)

    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows)
    ClosingIterator.apply(updatedRows.iterator)
  }

  private def getResultMap(
    func: FunctionInfo,
    allowedExecute: Boolean,
    executeRoles: Set[String],
    boostedExecuteRoles: Set[String],
    allowShowRoles: Boolean
  ): Map[String, AnyValue] =
    if (allowedExecute) getResultMap(func, executeRoles, boostedExecuteRoles, allowShowRoles)
    else Map.empty[String, AnyValue]

  private def getResultMap(
    func: FunctionInfo,
    executeRoles: Set[String],
    boostedExecuteRoles: Set[String],
    allowShowRoles: Boolean
  ): Map[String, AnyValue] = {
    val (rolesList, boostedRolesList) =
      if (rolesColumnRequested && allowShowRoles)
        ShowProcFuncCommandHelper.roleValues(executeRoles, boostedExecuteRoles)
      else (Values.NO_VALUE, Values.NO_VALUE)

    requestedColumnsNames.map {
      // Name of the function, for example "my.func"
      case `nameColumn` => nameColumn -> Values.stringValue(func.name)
      // Function category, for example Numeric or Temporal
      case `categoryColumn` => categoryColumn -> Values.stringValue(func.category)
      // Function description or empty string
      case `descriptionColumn` => descriptionColumn -> Values.stringValue(func.description)
      // Function signature
      case `signatureColumn` => signatureColumn -> Values.stringValue(func.signature)
      // Tells if the function is built in or user defined
      case `isBuiltInColumn` => isBuiltInColumn -> Values.booleanValue(func.isBuiltIn)
      // Lists of arguments, as map of strings with name, type, default and description
      case `argumentDescriptionColumn` =>
        argumentDescriptionColumn -> ShowProcFuncCommandHelper.fieldDescriptions(func.argDescr)
      // Return value type
      case `returnDescriptionColumn` => returnDescriptionColumn -> Values.stringValue(func.retDescr)
      // Tells the function is aggregating
      case `aggregatingColumn` => aggregatingColumn -> Values.booleanValue(func.aggregating)
      // List of roles that can execute the function
      case `rolesExecutionColumn` => rolesExecutionColumn -> rolesList
      // List of roles that can execute the function with boosted privileges
      case `rolesBoostedExecutionColumn` => rolesBoostedExecutionColumn -> boostedRolesList
      // Tells if the function is deprecated
      case `isDeprecatedColumn` => isDeprecatedColumn -> Values.booleanValue(func.deprecated)
      case `deprecatedByColumn` => deprecatedByColumn -> Values.stringOrNoValue(func.deprecatedBy)
      case unknown              =>
        // This match should cover all existing columns but we get scala warnings
        // on non-exhaustive match due to it being string values
        throw new IllegalStateException(s"Missing case for column: $unknown")
    }.toMap[String, AnyValue]
  }

  private def getAllRoles(systemGraph: GraphDatabaseService): Set[String] = {
    val stx = systemGraph.beginTx()
    val roleNames = stx.execute("SHOW ALL ROLES YIELD role").columnAs[String]("role").asScala.toSet
    stx.commit()
    roleNames
  }

  private case class FunctionInfo(
    name: String,
    category: String,
    description: String,
    signature: String,
    isBuiltIn: Boolean,
    argDescr: List[InputInformation],
    retDescr: String,
    aggregating: Boolean,
    deprecated: Boolean,
    deprecatedBy: String
  )

  private object FunctionInfo {

    def apply(info: UserFunctionSignature, aggregating: Boolean): FunctionInfo = {
      val name = info.name.toString
      val category = info.category.orElse("")
      val description = info.description.orElse("")
      val signature = info.toString
      val isBuiltIn = info.isBuiltIn
      val argumentDescr = ShowProcFuncCommandHelper.getSignatureValues(info.inputSignature())
      val returnDescr = info.outputType.toString
      val deprecated = info.isDeprecated
      val deprecatedBy = if (info.deprecated() == null || info.deprecated.isEmpty) null else info.deprecated.get
      FunctionInfo(
        name,
        category,
        description,
        signature,
        isBuiltIn,
        argumentDescr,
        returnDescr,
        aggregating,
        deprecated,
        deprecatedBy
      )
    }

    def apply(info: FunctionInformation): FunctionInfo = {
      val name = info.getFunctionName
      val category = info.getCategory
      val signature = info.getSignature
      val description = info.getDescription
      val aggregating = info.isAggregationFunction
      val argumentDescr = info.inputSignature.asScala.toList
      val returnDescr = info.returnType
      val deprecated = info.isDeprecated
      val deprecatedBy = if (info.deprecatedBy() == null || info.deprecatedBy.isEmpty) null else info.deprecatedBy.get
      FunctionInfo(
        name,
        category,
        description,
        signature,
        isBuiltIn = true,
        argumentDescr,
        returnDescr,
        aggregating,
        deprecated,
        deprecatedBy
      )
    }
  }
}
