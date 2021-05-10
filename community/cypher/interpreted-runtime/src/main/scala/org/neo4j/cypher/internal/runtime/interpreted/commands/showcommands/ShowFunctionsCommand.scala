/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowFunctionType
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommunityCypherRowFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.QueryExecutionEngine
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

// SHOW [ALL | BUILT IN | USER DEFINED] FUNCTION[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
case class ShowFunctionsCommand(functionType: ShowFunctionType, executableBy: Option[ExecutableBy], verbose: Boolean, columns: Set[ShowColumn]) extends Command(columns) {
  override def originalNameRows(state: QueryState): ClosingIterator[Map[String, AnyValue]] = {
    val isCommunity = state.rowFactory.isInstanceOf[CommunityCypherRowFactory]
    val (privileges, systemGraph) =
      if (!isCommunity && (verbose || executableBy.isDefined)) ShowProcFuncCommandHelper.getPrivileges(state, "FUNCTION")  // Always give Some(_: GraphDatabaseService)
      else (ShowProcFuncCommandHelper.Privileges(List.empty, List.empty, List.empty, List.empty), None)

    val tx = state.query.transactionalContext.transaction
    val securityContext = tx.securityContext()
    val (userRoles, alwaysExecutable) =
      if (!isCommunity) {
        ShowProcFuncCommandHelper.getRolesForUser(securityContext, tx.securityAuthorizationHandler(), systemGraph, executableBy, "SHOW FUNCTIONS")
      } else {
        (Set.empty[String], true)
      }
    val allowShowRoles = if (!isCommunity && verbose) securityContext.allowsAdminAction(new AdminActionOnResource(SHOW_ROLE, DatabaseScope.ALL, Segment.ALL)) else false

    // gets you all functions provided by the query language
    val languageFunctions = functionType match {
      case UserDefinedFunctions => List.empty // Will anyway filter out all built-in functions and all of these are built-in
      case _ => state.query.graph().getDependencyResolver.resolveDependency(classOf[QueryExecutionEngine]).getProvidedLanguageFunctions.asScala.map(f => FunctionInfo(f)).toList
    }

    // gets you all non-aggregating functions that are registered in the db (incl. those from libs like apoc)
    val loadedFunctions = tx.procedures().functionGetAll().iterator.asScala.map(f => FunctionInfo(f, aggregating = false)).toList

    // gets you all aggregation functions that are registered in the db (incl. those from libs like apoc)
    val loadedAggregationFunctions = tx.procedures().aggregationFunctionGetAll().iterator.asScala.map(f => FunctionInfo(f, aggregating = true)).toList

    val allFunctions = languageFunctions ++ loadedFunctions ++ loadedAggregationFunctions
    val filteredFunctions = functionType match {
      case AllFunctions         => allFunctions
      case BuiltInFunctions     => allFunctions.filter(f => f.isBuiltIn)
      case UserDefinedFunctions => allFunctions.filter(f => !f.isBuiltIn)
    }
    val sortedFunctions = filteredFunctions.sortBy(a => a.name)

    val rows = sortedFunctions.map { func =>
      val (executeRoles, boostedExecuteRoles, allowedExecute) =
        if (!isCommunity && (verbose || executableBy.isDefined)) {
          if (func.isBuiltIn) getRolesForBuiltIn(func.name, privileges, systemGraph, userRoles.nonEmpty)
          else ShowProcFuncCommandHelper.roles(func.name, isAdmin = false, privileges, userRoles) // There is no admin functions (only applicable to procedures)
        } else (Set.empty[String], Set.empty[String], isCommunity)

      executableBy match {
        case Some(_) => getResultMap(func, alwaysExecutable || allowedExecute, executeRoles, boostedExecuteRoles, allowShowRoles)
        case None => getResultMap(func, executeRoles, boostedExecuteRoles, allowShowRoles)
      }
    }.filter(m => m.nonEmpty)

    ClosingIterator.apply(rows.iterator)
  }

  private def getResultMap(func: FunctionInfo,
                           allowedExecute: Boolean,
                           executeRoles: Set[String],
                           boostedExecuteRoles: Set[String],
                           allowShowRoles: Boolean): Map[String, AnyValue] =
    if (allowedExecute) getResultMap(func, executeRoles, boostedExecuteRoles, allowShowRoles)
    else Map.empty[String, AnyValue]

  private def getResultMap(func: FunctionInfo,
                           executeRoles: Set[String],
                           boostedExecuteRoles: Set[String],
                           allowShowRoles: Boolean): Map[String, AnyValue] = {
    val briefResult = Map(
      // Name of the function, for example "my.func"
      "name" -> Values.stringValue(func.name),
      // Function category, for example Numeric or Temporal
      "category" -> Values.stringValue(func.category),
      // Function description or empty string
      "description" -> Values.stringValue(func.description),
    )
    if (verbose) {
      val (rolesList, boostedRolesList) =
        if (allowShowRoles) ShowProcFuncCommandHelper.roleValues(executeRoles, boostedExecuteRoles) else (Values.NO_VALUE, Values.NO_VALUE)

      briefResult ++ Map(
        // Function signature
        "signature" -> Values.stringValue(func.signature),
        // Tells if the function is built in or user defined
        "isBuiltIn" -> Values.booleanValue(func.isBuiltIn),
        // Lists of arguments, as map of strings with name, type, default and description
        "argumentDescription" -> ShowProcFuncCommandHelper.fieldDescriptions(func.argDescr),
        // Return value type
        "returnDescription" -> Values.stringValue(func.retDescr),
        // Tells the function is aggregating
        "aggregating" -> Values.booleanValue(func.aggregating),
        // List of roles that can execute the function
        "rolesExecution" -> rolesList,
        // List of roles that can execute the function with boosted privileges
        "rolesBoostedExecution" -> boostedRolesList
      )
    } else {
      briefResult
    }
  }

  private def getRolesForBuiltIn(name: String,
                                 privileges: ShowProcFuncCommandHelper.Privileges,
                                 systemGraph: Option[GraphDatabaseService],
                                 allowedExecute: Boolean): (Set[String], Set[String], Boolean) = {
    val allowedBoostedRoles = privileges.grantedBoostedExecuteRoles(name) -- privileges.deniedBoostedExecuteRoles(name)
    (getAllRoles(systemGraph), allowedBoostedRoles, allowedExecute)
  }

  private def getAllRoles(systemGraph: Option[GraphDatabaseService]): Set[String] = {
    val stx = systemGraph.get.beginTx() // Will be Some(_: GraphDatabaseService) since either verbose or executableBy.isDefined
    val roleNames = stx.execute("SHOW ALL ROLES YIELD role").columnAs[String]("role").asScala.toSet
    stx.commit()
    roleNames
  }

  private case class FunctionInfo(name: String, category: String, description: String, signature: String, isBuiltIn: Boolean,
                                  argDescr: List[Map[String, String]], retDescr: String, aggregating: Boolean)

  private object FunctionInfo {
    def apply(info: UserFunctionSignature, aggregating: Boolean): FunctionInfo = {
      val name = info.name.toString
      val category = info.category.orElse("")
      val description = info.description.orElse("")
      val signature = info.toString
      val isBuiltIn = info.isBuiltIn
      val argumentDescr = ShowProcFuncCommandHelper.getSignatureValues(info.inputSignature())
      val returnDescr = info.outputType.toString
      FunctionInfo(name, category, description, signature, isBuiltIn, argumentDescr, returnDescr, aggregating)
    }

    def apply(info: FunctionInformation): FunctionInfo = {
      val name = info.getFunctionName
      val category = info.getCategory
      val signature = info.getSignature
      val description = info.getDescription
      val aggregating = info.isAggregationFunction
      val argumentDescr = info.inputSignature.asScala.map(m => m.asScala.toMap).toList
      val returnDescr = info.returnType
      FunctionInfo(name, category, description, signature, isBuiltIn = true, argumentDescr, returnDescr, aggregating)
    }
  }
}
