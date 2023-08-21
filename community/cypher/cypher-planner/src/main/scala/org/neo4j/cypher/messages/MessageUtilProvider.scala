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
