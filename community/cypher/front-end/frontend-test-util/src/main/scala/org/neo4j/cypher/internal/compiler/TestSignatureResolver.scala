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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage.Cypher25
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName

trait TestSignatureResolver(
  procSignatureLookup: ProcedureName => ProcedureSignature,
  funcSignatureLookup: FunctionName => Option[UserFunctionSignature]
) extends ScopedProcedureSignatureResolver {
  override def procedureSignature(name: ProcedureName): ProcedureSignature = procSignatureLookup(name)

  override def functionSignature(name: FunctionName): Option[UserFunctionSignature] = funcSignatureLookup(name)

  override def procedureSignatureVersion: Long = -1

  override def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature] = None

  override def queryLanguage: QueryLanguage = Cypher25
}
