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

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

case class SystemProcedureCallPlanner(
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler
) {

  def planSystemProcedureCall(
    call: ResolvedCall,
    returns: Option[Return],
    checkCredentialsExpired: Boolean
  ): ExecutionPlan = {
    val queryString = returns match {
      case Some(rs @ Return(_, ReturnItems(_, items, _), _, _, _, _, _)) if items.nonEmpty =>
        QueryRenderer.render(Seq(call, rs))
      case _ => QueryRenderer.render(Seq(call))
    }

    def addParameterDefaults(params: MapValue): MapValue = {
      val builder = call.folder.treeFold(new MapValueBuilder()) {
        case ImplicitProcedureArgument(name, _, defaultValue) => acc =>
            acc.add(name, ValueUtils.of(defaultValue))
            TraverseChildren(acc)
      }
      val defaults = builder.build()
      defaults.updatedWith(params)
    }

    SystemCommandExecutionPlan(
      "SystemProcedure",
      normalExecutionEngine,
      securityAuthorizationHandler,
      queryString,
      MapValue.EMPTY,
      checkCredentialsExpired = checkCredentialsExpired,
      parameterTransformer = ParameterTransformer().convert((_, params) => addParameterDefaults(params)),
      modeConverter = s => s.withMode(new OverriddenAccessMode(s.mode(), AccessMode.Static.READ))
    )
  }

}
