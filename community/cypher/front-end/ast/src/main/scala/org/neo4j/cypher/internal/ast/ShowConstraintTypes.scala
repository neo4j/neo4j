/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

sealed trait ShowConstraintType {
  val output: String
  val prettyPrint: String
  val description: String
}

case object AllConstraints extends ShowConstraintType {
  override val output: String = "ALL"
  override val prettyPrint: String = "ALL"
  override val description: String = "allConstraints"
  def instance: ShowConstraintType = this // to reach from Java
}

case object UniqueConstraints extends ShowConstraintType {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "UNIQUE"
  override val description: String = "uniquenessConstraints"
  def instance: ShowConstraintType = this // to reach from Java
}

case class ExistsConstraints(syntax: ExistenceConstraintSyntax) extends ShowConstraintType {
  override val output: String = "PROPERTY_EXISTENCE"
  override val prettyPrint: String = syntax.keyword
  override val description: String = "existenceConstraints"
}

case class NodeExistsConstraints(syntax: ExistenceConstraintSyntax = NewSyntax) extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_EXISTENCE"
  override val prettyPrint: String = s"NODE ${syntax.keyword}"
  override val description: String = "nodeExistenceConstraints"
}

case class RelExistsConstraints(syntax: ExistenceConstraintSyntax = NewSyntax) extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_EXISTENCE"
  override val prettyPrint: String = s"RELATIONSHIP ${syntax.keyword}"
  override val description: String = "relationshipExistenceConstraints"
}

case object NodeKeyConstraints extends ShowConstraintType {
  override val output: String = "NODE_KEY"
  override val prettyPrint: String = "NODE KEY"
  override val description: String = "nodeKeyConstraints"
  def instance: ShowConstraintType = this // to reach from Java
}

sealed trait ExistenceConstraintSyntax {
  val keyword: String
}

case object DeprecatedSyntax extends ExistenceConstraintSyntax {
  override val keyword: String = "EXISTS"
  def instance: ExistenceConstraintSyntax = this // to reach from Java
}

case object OldValidSyntax extends ExistenceConstraintSyntax {
  override val keyword: String = "EXIST"
  def instance: ExistenceConstraintSyntax = this // to reach from Java
}

case object NewSyntax extends ExistenceConstraintSyntax {
  override val keyword: String = "EXISTENCE"
  def instance: ExistenceConstraintSyntax = this // to reach from Java
}
