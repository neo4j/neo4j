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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorMessageHolder
import org.neo4j.gqlstatus.HasGqlStatusInfo
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus

class Neo4jASTConstructionException(errorGqlStatusObject: ErrorGqlStatusObject, msg: String)
    extends RuntimeException(ErrorMessageHolder.getMessage(errorGqlStatusObject, msg))
    with HasStatus with HasGqlStatusInfo {
  def this(msg: String) = this(null, msg)
  override def status(): Status = Status.Statement.SyntaxError
  override def gqlStatusObject(): ErrorGqlStatusObject = errorGqlStatusObject
  override def getOldMessage: String = msg
}

object Neo4jASTConstructionException {

  def apply(msg: String): Neo4jASTConstructionException = {
    new Neo4jASTConstructionException(null, msg)
  }
}
