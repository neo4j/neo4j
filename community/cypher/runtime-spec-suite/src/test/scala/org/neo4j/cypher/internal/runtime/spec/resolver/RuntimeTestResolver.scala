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
package org.neo4j.cypher.internal.runtime.spec.resolver

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext

trait RuntimeTestResolver[CONTEXT <: RuntimeContext] extends Resolver {

  protected def runtimeTestSupport: RuntimeTestSupport[CONTEXT]

  private def tx = runtimeTestSupport.tx

  override def getLabelId(label: String): Int = {
    tx.kernelTransaction().tokenRead().nodeLabel(label)
  }

  override def getRelTypeId(relType: String): Int = {
    tx.kernelTransaction().tokenRead().relationshipType(relType)
  }

  override def getPropertyKeyId(prop: String): Int = {
    tx.kernelTransaction().tokenRead().propertyKey(prop)
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature = {
    val ktx = tx.kernelTransaction()
    TransactionBoundPlanContext.procedureSignature(ktx, name, CypherVersion.Default)
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val ktx = tx.kernelTransaction()
    TransactionBoundPlanContext.functionSignature(ktx, name, CypherVersion.Default)
  }
}
