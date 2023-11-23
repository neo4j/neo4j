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
package org.neo4j.cypher.messages

import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.messages.MessageUtil

object MessageUtilProvider extends ErrorMessageProvider {

  override def createMissingPropertyLabelHintError(
    operatorDescription: String,
    hintStringification: String,
    missingThingDescription: String,
    foundThingsDescription: String,
    entityDescription: String,
    entityName: String,
    additionalInfo: String
  ): String =
    MessageUtil.createMissingPropertyLabelHintError(
      operatorDescription,
      hintStringification,
      missingThingDescription,
      foundThingsDescription,
      entityDescription,
      entityName,
      additionalInfo
    )

  override def createSelfReferenceError(name: String): String = {
    MessageUtil.createSelfReferenceError(name)
  }

  override def createSelfReferenceError(name: String, variableType: String): String = {
    MessageUtil.createSelfReferenceError(name, variableType)
  }

  override def createUseClauseUnsupportedError(): String =
    "The USE clause is not available in embedded sessions. Try running the query using a Neo4j driver or the HTTP API."

  override def createDynamicGraphReferenceUnsupportedError(): String =
    "Dynamic graph references are supported only in composite databases."

  override def createMultipleGraphReferencesError(): String = {
    "Multiple graph references in the same query is not supported on standard databases. This capability is supported on composite databases only."
  }
}
