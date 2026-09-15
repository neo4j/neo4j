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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.CommandClause.checkIdentifierForVariableShadowing
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics._
import org.neo4j.cypher.internal.util.InputPosition

sealed trait ExecutableBy extends SemanticCheckable {
  def description(forType: String): String

  override def semanticCheck: SemanticCheck = SemanticCheck.success
}

object ExecutableBy {
  def defaultDescription(forType: String): String = s"${forType}ForUser(all)"
}

case object CurrentUser extends ExecutableBy {
  override def description(forType: String): String = s"${forType}ForUser(current)"
}

case class User(name: String)(val position: InputPosition) extends ExecutableBy {
  override def description(forType: String): String = s"${forType}ForUser($name)"

  override def semanticCheck: SemanticCheck = checkIdentifierForVariableShadowing(name, "EXECUTABLE BY", position)
}
