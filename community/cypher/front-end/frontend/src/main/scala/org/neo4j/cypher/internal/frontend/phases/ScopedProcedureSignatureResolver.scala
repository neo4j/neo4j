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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName

trait ProcedureSignatureResolver {
  def procedureSignature(name: ProcedureName, scope: QueryLanguage): ProcedureSignature
  def functionSignature(name: FunctionName, scope: QueryLanguage): Option[UserFunctionSignature]
  def procedureSignatureVersion: Long
}

trait ScopedProcedureSignatureResolver {
  def procedureSignature(name: ProcedureName): ProcedureSignature
  def functionSignature(name: FunctionName): Option[UserFunctionSignature]
  def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature]
  def procedureSignatureVersion: Long
  def queryLanguage: QueryLanguage
}

object ScopedProcedureSignatureResolver {

  val NoResolver: ScopedProcedureSignatureResolver = new ScopedProcedureSignatureResolver {
    override def procedureSignature(name: ProcedureName): ProcedureSignature =
      throw new UnsupportedOperationException("No procedure resolver available")
    override def functionSignature(name: FunctionName): Option[UserFunctionSignature] =
      throw new UnsupportedOperationException("No function resolver available")
    override def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature] = None
    override def procedureSignatureVersion: Long =
      throw new UnsupportedOperationException("No signature version available")
    override def queryLanguage: QueryLanguage = QueryLanguage.Cypher25
  }

  def from(r: ProcedureSignatureResolver, scope: QueryLanguage): ScopedProcedureSignatureResolver = {
    val other = QueryLanguage.otherVersion(scope)
    new ScopedProcedureSignatureResolver {
      override def procedureSignature(n: ProcedureName): ProcedureSignature = r.procedureSignature(n, scope)
      override def functionSignature(n: FunctionName): Option[UserFunctionSignature] = r.functionSignature(n, scope)
      override def functionSignatureInOtherVersion(n: FunctionName): Option[UserFunctionSignature] =
        r.functionSignature(n, other)
      override def procedureSignatureVersion: Long = r.procedureSignatureVersion
      override def queryLanguage: QueryLanguage = scope
    }
  }
}
