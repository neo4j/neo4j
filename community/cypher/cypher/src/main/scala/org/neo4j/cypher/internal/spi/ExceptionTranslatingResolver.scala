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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.KernelException

class ExceptionTranslatingResolver(
  inner: ScopedProcedureSignatureResolver
) extends ScopedProcedureSignatureResolver {

  private def translateException[A](f: => A): A =
    try f
    catch {
      case e: KernelException =>
        throw CypherExecutionException.wrapKernelException(e.getMessage, e)
    }

  override def procedureSignature(name: ProcedureName): ProcedureSignature =
    translateException(inner.procedureSignature(name))

  override def functionSignature(name: FunctionName): Option[UserFunctionSignature] =
    translateException(inner.functionSignature(name))

  override def procedureSignatureVersion: Long = inner.procedureSignatureVersion

  override def queryLanguage: QueryLanguage = inner.queryLanguage

  override def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature] =
    translateException(inner.functionSignatureInOtherVersion(name))
}
