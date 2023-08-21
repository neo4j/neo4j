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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.values.virtual.MapValue

/**
 * This class should only be used to transport a system procedure query to the ManagementCommandRuntime.
 * Check that the query is an allowed system-only query BEFORE creating a SystemProcedureCall
 */
case class SystemProcedureCall(
  procedureName: String,
  call: ResolvedCall,
  returns: Option[Return],
  params: MapValue,
  checkCredentialsExpired: Boolean
)(implicit idGen: IdGen)
    extends LogicalPlanExtension(idGen) {

  override def lhs: Option[LogicalPlan] = None
  override def rhs: Option[LogicalPlan] = None
  override def availableSymbols: Set[LogicalVariable] = Set.empty
}
