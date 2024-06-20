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

trait ProcedureSignatureResolver {
  def procedureSignature(name: QualifiedName, scope: CypherScope): ProcedureSignature
  def functionSignature(name: QualifiedName, scope: CypherScope): Option[UserFunctionSignature]
  def procedureSignatureVersion: Long
}

trait ScopedProcedureSignatureResolver {
  def procedureSignature(name: QualifiedName): ProcedureSignature
  def functionSignature(name: QualifiedName): Option[UserFunctionSignature]
  def procedureSignatureVersion: Long
}

object ScopedProcedureSignatureResolver {

  def from(r: ProcedureSignatureResolver, scope: CypherScope): ScopedProcedureSignatureResolver = {
    new ScopedProcedureSignatureResolver {
      override def procedureSignature(n: QualifiedName): ProcedureSignature = r.procedureSignature(n, scope)
      override def functionSignature(n: QualifiedName): Option[UserFunctionSignature] = r.functionSignature(n, scope)
      override def procedureSignatureVersion: Long = r.procedureSignatureVersion
    }
  }
}
