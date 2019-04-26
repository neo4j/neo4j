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
import org.neo4j.values.storable.{TextValue, Values}
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
    // SHOW USERS
    case ShowUsers() => (_, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |OPTIONAL MATCH (u)-[:HAS_ROLE]->(r:Role)
          |RETURN u.name as user, collect(r.name) as roles""".stripMargin,
        VirtualValues.EMPTY_MAP
      )

    // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
    case ShowRoles(withUsers, showAll) => (_, _) =>
      // TODO fix the predefined roles to be connected to PredefinedRoles (aka PredefinedRoles.ADMIN)
      val predefinedRoles = Values.stringArray("admin", "architect", "publisher", "editor", "reader")
      val query = if (showAll)
                        """MATCH (r:Role)
                          |OPTIONAL MATCH (u:User)-[:HAS_ROLE]->(r)
                          |RETURN DISTINCT r.name as role,
                          |CASE
                          | WHEN r.name IN $predefined THEN true
                          | ELSE false
                          |END as is_built_in
                        """.stripMargin
                      else
                        """MATCH (r:Role)<-[:HAS_ROLE]-(u:User)
                          |RETURN DISTINCT r.name as role,
                          |CASE
                          | WHEN r.name IN $predefined THEN true
                          | ELSE false
                          |END as is_built_in
                        """.stripMargin
      if (withUsers) {
        SystemCommandExecutionPlan("ShowRoles", normalExecutionEngine, query + ", u.name as member",
          VirtualValues.map(Array("predefined"), Array(predefinedRoles))
        )
      } else {
        SystemCommandExecutionPlan("ShowRoles", normalExecutionEngine, query,
          VirtualValues.map(Array("predefined"), Array(predefinedRoles))
        )
      }

    // CREATE ROLE foo AS COPY OF bar
    case CreateRole(roleName, Some(from)) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """OPTIONAL MATCH (f:Role {name:$from})
          |// FOREACH is an IF, only create the role if the 'from' node exists
          |FOREACH (ignoreMe in CASE WHEN f IS NOT NULL THEN [1] ELSE [] END |
          | CREATE (r:Role)
          | SET r.name = $name
          |)
          |WITH f
          |// Make sure the create is eagerly done
          |OPTIONAL MATCH (r:Role {name:$name})
          |RETURN r.name as name, f.name as old""".stripMargin,
        VirtualValues.map(Array("name", "from"), Array(Values.stringValue(roleName), Values.stringValue(from))),
        record => {
          if (record.get("old") == null) throw new IllegalStateException("Cannot create role '" + roleName + "' from non-existent role '" + from + "'")
        }
      )

    // CREATE ROLE foo
    case CreateRole(roleName, _) => (_, _) =>
      SystemCommandExecutionPlan("CreateRole", normalExecutionEngine,
        """CREATE (r:Role)
          |SET r.name = $name
          |RETURN r.name as name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName)))
      )

    // DROP ROLE foo
    case DropRole(roleName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("DropRole", normalExecutionEngine,
        """OPTIONAL MATCH (r:Role {name:$name})
          |WITH r, r.name as name
          |DETACH DELETE r
          |RETURN name""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(roleName))),
        record => {
          if (record.get("name") == null) throw new IllegalStateException("Cannot drop non-existent role '" + roleName + "'")
        }
      )

    // SHOW DATABASES
    case ShowDatabases() => (_, _) =>
      SystemCommandExecutionPlan("ShowDatabases", normalExecutionEngine,
        "MATCH (d:Database) RETURN d.name as name, d.status as status", VirtualValues.emptyMap())

    // SHOW DATABASE foo
    case ShowDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("ShowDatabase", normalExecutionEngine,
        "MATCH (d:Database {name: $name}) RETURN d.name as name, d.status as status", VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))))

    // CREATE DATABASE foo
    case CreateDatabase(dbName) => (_, _) =>
      SystemCommandExecutionPlan("CreateDatabase", normalExecutionEngine,
        """CREATE (d:Database {name: $name})
          |SET d.status = $status
          |SET d.created_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name", "status"), Array(Values.stringValue(dbName), DatabaseStatus.Online))
      )

    // DROP DATABASE foo
    case DropDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("DropDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |REMOVE d:Database
          |SET d:DeletedDatabase
          |SET d.deleted_at = datetime()
          |RETURN d.name as name, d.status as status""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(dbName))),
        record => {
          if (record.get("name") == null) throw new IllegalStateException("Cannot drop non-existent database '" + dbName + "'")
        }
      )

    // START DATABASE foo
    case StartDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("StartDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.started_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            DatabaseStatus.Offline,
            DatabaseStatus.Online
          )
        ),
        record => {
          if (record.get("db") == null) throw new IllegalStateException("Cannot start non-existent database '" + dbName + "'")
        }
      )

    // STOP DATABASE foo
    case StopDatabase(dbName) => (_, _) =>
      UpdatingSystemCommandExecutionPlan("StopDatabase", normalExecutionEngine,
        """OPTIONAL MATCH (d:Database {name: $name})
          |OPTIONAL MATCH (d2:Database {name: $name, status: $oldStatus})
          |SET d2.status = $status
          |SET d2.stopped_at = datetime()
          |RETURN d2.name as name, d2.status as status, d.name as db""".stripMargin,
        VirtualValues.map(
          Array("name", "oldStatus", "status"),
          Array(Values.stringValue(dbName),
            DatabaseStatus.Online,
            DatabaseStatus.Offline
          )
        ),
        record => {
          if (record.get("db") == null) throw new IllegalStateException("Cannot stop non-existent database '" + dbName + "'")
        }
      )
  }
}

object MultiDatabaseManagementCommandRuntime {
  def isApplicable(logicalPlanState: LogicalPlanState): Boolean =
    MultiDatabaseManagementCommandRuntime(null).logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}

object DatabaseStatus extends Enumeration {
  type Status = TextValue

  val Deleted: TextValue = Values.stringValue("deleted")
  val Online: TextValue = Values.stringValue("online")
  val Offline: TextValue = Values.stringValue("offline")
}
