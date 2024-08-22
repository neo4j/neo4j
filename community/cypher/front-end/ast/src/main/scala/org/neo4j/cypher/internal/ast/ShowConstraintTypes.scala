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

sealed trait ShowConstraintType {
  val output: String
  val prettyPrint: String
  val description: String
}

case object AllConstraints extends ShowConstraintType {
  override val output: String = "ALL"
  override val prettyPrint: String = "ALL"
  override val description: String = "allConstraints"
}

sealed trait UniqueConstraints extends ShowConstraintType

object UniqueConstraints {
  def cypher6: UniqueConstraints = UniqueConstraintsCypher6
  def cypher5: UniqueConstraints = UniqueConstraintsCypher5
}

private case object UniqueConstraintsCypher6 extends UniqueConstraints {
  override val output: String = "PROPERTY_UNIQUENESS"
  override val prettyPrint: String = "PROPERTY UNIQUENESS"
  override val description: String = "propertyUniquenessConstraints"
}

private case object UniqueConstraintsCypher5 extends UniqueConstraints {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "UNIQUENESS"
  override val description: String = "uniquenessConstraints"
}

sealed trait NodeUniqueConstraints extends ShowConstraintType

object NodeUniqueConstraints {
  def cypher6: NodeUniqueConstraints = NodeUniqueConstraintsCypher6
  def cypher5: NodeUniqueConstraints = NodeUniqueConstraintsCypher5
}

private case object NodeUniqueConstraintsCypher6 extends NodeUniqueConstraints {
  override val output: String = "NODE_PROPERTY_UNIQUENESS"
  override val prettyPrint: String = "NODE PROPERTY UNIQUENESS"
  override val description: String = "nodePropertyUniquenessConstraints"
}

private case object NodeUniqueConstraintsCypher5 extends NodeUniqueConstraints {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "NODE UNIQUENESS"
  override val description: String = "nodeUniquenessConstraints"
}

sealed trait RelUniqueConstraints extends ShowConstraintType

object RelUniqueConstraints {
  def cypher6: RelUniqueConstraints = RelUniqueConstraintsCypher6
  def cypher5: RelUniqueConstraints = RelUniqueConstraintsCypher5
}

private case object RelUniqueConstraintsCypher6 extends RelUniqueConstraints {
  override val output: String = "RELATIONSHIP_PROPERTY_UNIQUENESS"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY UNIQUENESS"
  override val description: String = "relationshipPropertyUniquenessConstraints"
}

private case object RelUniqueConstraintsCypher5 extends RelUniqueConstraints {
  override val output: String = "RELATIONSHIP_UNIQUENESS"
  override val prettyPrint: String = "RELATIONSHIP UNIQUENESS"
  override val description: String = "relationshipUniquenessConstraints"
}

sealed trait ExistsConstraints extends ShowConstraintType {
  override val output: String = "PROPERTY_EXISTENCE"
  override val prettyPrint: String = "PROPERTY EXISTENCE"
}

object ExistsConstraints {
  def cypher6: ExistsConstraints = ExistsConstraintsCypher6
  def cypher5: ExistsConstraints = ExistsConstraintsCypher5
}

private case object ExistsConstraintsCypher6 extends ExistsConstraints {
  override val description: String = "propertyExistenceConstraints"
}

private case object ExistsConstraintsCypher5 extends ExistsConstraints {
  override val description: String = "existenceConstraints"
}

sealed trait NodeExistsConstraints extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_EXISTENCE"
  override val prettyPrint: String = "NODE PROPERTY EXISTENCE"
}

object NodeExistsConstraints {
  def cypher6: NodeExistsConstraints = NodeExistsConstraintsCypher6
  def cypher5: NodeExistsConstraints = NodeExistsConstraintsCypher5
}

private case object NodeExistsConstraintsCypher6 extends NodeExistsConstraints {
  override val description: String = "nodePropertyExistenceConstraints"
}

private case object NodeExistsConstraintsCypher5 extends NodeExistsConstraints {
  override val description: String = "nodeExistenceConstraints"
}

sealed trait RelExistsConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_EXISTENCE"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY EXISTENCE"
}

object RelExistsConstraints {
  def cypher6: RelExistsConstraints = RelExistsConstraintsCypher6
  def cypher5: RelExistsConstraints = RelExistsConstraintsCypher5
}

private case object RelExistsConstraintsCypher6 extends RelExistsConstraints {
  override val description: String = "relationshipPropertyExistenceConstraints"
}

private case object RelExistsConstraintsCypher5 extends RelExistsConstraints {
  override val description: String = "relationshipExistenceConstraints"
}

case object KeyConstraints extends ShowConstraintType {
  override val output: String = "KEY"
  override val prettyPrint: String = "KEY"
  override val description: String = "keyConstraints"
}

case object NodeKeyConstraints extends ShowConstraintType {
  override val output: String = "NODE_KEY"
  override val prettyPrint: String = "NODE KEY"
  override val description: String = "nodeKeyConstraints"
}

case object RelKeyConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_KEY"
  override val prettyPrint: String = "RELATIONSHIP KEY"
  override val description: String = "relationshipKeyConstraints"
}

case object PropTypeConstraints extends ShowConstraintType {
  override val output: String = "PROPERTY_TYPE"
  override val prettyPrint: String = "PROPERTY TYPE"
  override val description: String = "propertyTypeConstraints"
}

case object NodePropTypeConstraints extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_TYPE"
  override val prettyPrint: String = "NODE PROPERTY TYPE"
  override val description: String = "nodePropertyTypeConstraints"
}

case object RelPropTypeConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_TYPE"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY TYPE"
  override val description: String = "relationshipPropertyTypeConstraints"
}

case object RelationshipEndpointConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_ENDPOINT_LABEL"
  override val prettyPrint: String = "RELATIONSHIP ENDPOINT LABEL"
  override val description: String = "relationshipEndpointLabelConstraints"
}

case object LabelCoexistenceConstraints extends ShowConstraintType {
  // TODO: Verify once CIP: https://docs.google.com/document/d/1UcqorA0YmqgE-Pvs6zDxsGzoC2sKsGYgT5qDkXeQX6Q/edit is approved
  override val output: String = "NODE_LABEL_COEXISTENCE"
  override val prettyPrint: String = "NODE LABEL COEXISTENCE"
  override val description: String = "nodeLabelCoexistenceConstraints"
}
