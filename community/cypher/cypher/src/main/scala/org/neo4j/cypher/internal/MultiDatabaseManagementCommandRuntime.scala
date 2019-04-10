/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs.{SystemCommandExecutionPlan, UpdatingSystemCommandExecutionPlan}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

/**
  * This runtime takes on queries that require no planning, such as multidatabase management commands
  */
case class MultiDatabaseManagementCommandRuntime(normalExecutionEngine: ExecutionEngine) extends CypherRuntime[RuntimeContext] {
  override def name: String = "multidatabase-commands"

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {

    def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
      throw new CantCompileQueryException(
        s"Plan is not a recognized database administration command: ${unknownPlan.getClass.getSimpleName}")
    }

    val (withSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    logicalToExecutable.applyOrElse(withSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, Map[String, Int]) => ExecutionPlan] = {
    // SHOW DATABASES
    case ShowDatabases() => (_, _) =>
      SystemCommandExecutionPlan("ShowDatabases", normalExecutionEngine,
        "MATCH (d:Database) WHERE d.status <> 'deleted' RETURN d.name as name, d.status as status",
        VirtualValues.EMPTY_MAP
      )

    // SHOW DATABASE foo
    case ShowDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("ShowDatabase", normalExecutionEngine,
        "MATCH (d:Database {name:$name}) WHERE d.status <> 'deleted' RETURN d.name as name, d.status as status",
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName)))
      )

    // CREATE DATABASE foo
    case CreateDatabase(dbName) => (ctx, _) =>
      UpdatingSystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        "CREATE (d:Database {name:$name}) " +
          "SET d.status = $status RETURN d.name as name, d.status as status",
        VirtualValues.map(Array("name", "status"), Array(Values.stringValue(dbName), Values.stringValue("online"))),
        record => {}  //TODO: Make sure we have a uniqueness constraint on Database(name)
      )

    // DROP DATABASE foo
    case DropDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("DeleteDatabase", normalExecutionEngine,
        "OPTIONAL MATCH (d:Database {name:$name}), (d2: Database{name:$name}) " +
          "WHERE d2.status <> $requiredStatus "+
          "SET d2.status = $status " +
          "RETURN d.name as name, d.status as status, d2.name as db",
        VirtualValues.map(Array("name", "requiredStatus","status"), Array(Values.stringValue(dbName),
          Values.stringValue("offline"),
          Values.stringValue("deleted"))),
        record => {
          if (record.get("name") == null) throw new IllegalStateException("Cannot delete non existent database '" + dbName + "'")
          if (record.get("db") == null) throw new IllegalStateException("Cannot delete database '" + dbName + "' that is not offline. It is: " + record.get("status"))
        }
      )

    // START DATABASE foo
    case StartDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("StartDatabase", normalExecutionEngine,
        "MATCH (d:Database {name:$name, status: $oldStatus})" +
          " SET d.status = $status " +
          "RETURN d.name as name, d.status as status",
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            Values.stringValue("offline"),
            Values.stringValue("online")
          )
        )
      )

    // STOP DATABASE foo
    case StopDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        "MATCH (d:Database {name:$name, status:$oldStatus}) SET d.status = $status " +
          "RETURN d.name as name, d.status as status",
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            Values.stringValue("online"),
            Values.stringValue("offline")
          )
        )
      )
  }
}

object MultiDatabaseManagementCommandRuntime {
  def isApplicable(logicalPlanState: LogicalPlanState) =
    MultiDatabaseManagementCommandRuntime(null).logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}
