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
package org.neo4j.cypher.internal.administration

import org.neo4j.cypher.internal.AdministrationCommandRuntime.Show
import org.neo4j.cypher.internal.AdministrationCommandRuntime.Show.showDatabaseName
import org.neo4j.cypher.internal.AdministrationCommandRuntime.Show.showString
import org.neo4j.cypher.internal.AdministrationCommandRuntime.checkNamespaceExists
import org.neo4j.cypher.internal.AdministrationCommandRuntime.followerError
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getDatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userLabel
import org.neo4j.cypher.internal.AdministrationCommandRuntime.userNamePropKey
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.RBACEntity
import org.neo4j.cypher.internal.logical.plans.RoleEntity
import org.neo4j.cypher.internal.logical.plans.UserEntity
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL_DESCRIPTION
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.values.virtual.VirtualValues

case class EnsureNodeExistsExecutionPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planEnsureNodeExists(
    entity: RBACEntity,
    name: Either[String, Parameter],
    valueMapper: String => String,
    extraFilter: String => String,
    labelDescription: String,
    action: String,
    sourcePlan: Option[ExecutionPlan]
  ): ExecutionPlan = {
    val (label, namePropKey) = entity match {
      case UserEntity => (userLabel, userNamePropKey)
      case RoleEntity => ("Role", "name")
    }
    val nameFields = getNameFields("name", name, valueMapper = valueMapper)
    UpdatingSystemCommandExecutionPlan(
      "EnsureNodeExists",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (node:$label {$namePropKey: $$`${nameFields.nameKey}`})
         |${extraFilter("node")}
         |RETURN node""".stripMargin,
      VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
      queryHandler(action, labelDescription, name),
      sourcePlan,
      parameterTransformer = ParameterTransformer().convert(nameFields.nameConverter)
    )
  }

  def planEnsureDatabaseNodeExists(
    aliasName: DatabaseName,
    extraFilter: String => String,
    action: String,
    sourcePlan: Option[ExecutionPlan]
  ): ExecutionPlan = {
    val aliasNameFields =
      getDatabaseNameFields("aliasName", aliasName)

    UpdatingSystemCommandExecutionPlan(
      "EnsureNodeExists",
      normalExecutionEngine,
      securityAuthorizationHandler,
      s"""MATCH (node:$DATABASE_NAME ${aliasNameFields.asNodeFilter})
         |${extraFilter("node")}
         |RETURN node""".stripMargin,
      VirtualValues.map(aliasNameFields.keys, aliasNameFields.values),
      queryHandler(action, DATABASE_NAME_LABEL_DESCRIPTION, aliasName),
      sourcePlan,
      parameterTransformer = ParameterTransformer().convert(aliasNameFields.nameConverter).validate(
        checkNamespaceExists(aliasNameFields)
      )
    )

  }

  private def queryHandler[T](action: String, labelDescription: String, value: T)(implicit show: Show[T]) = {
    QueryHandler
      .handleNoResult(p =>
        Some(ThrowException(new InvalidArgumentException(
          s"Failed to $action the specified ${labelDescription.toLowerCase} '${show(value, p)}': $labelDescription does not exist."
        )))
      )
      .handleError {
        case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
          new DatabaseAdministrationOnFollowerException(
            s"Failed to $action the specified ${labelDescription.toLowerCase} '${show(value, p)}': $followerError",
            error
          )
        case (error, p) => new IllegalStateException(
            s"Failed to $action the specified ${labelDescription.toLowerCase} '${show(value, p)}'.",
            error
          ) // should not get here but need a default case
      }
  }
}
