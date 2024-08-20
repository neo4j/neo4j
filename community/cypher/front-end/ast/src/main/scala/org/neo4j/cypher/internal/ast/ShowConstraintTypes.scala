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

case object UniqueConstraints extends ShowConstraintType {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "UNIQUENESS"
  override val description: String = "uniquenessConstraints"
}

case object NodeUniqueConstraints extends ShowConstraintType {
  override val output: String = "UNIQUENESS" // cannot change constraint type until 6.0: update to `NODE_UNIQUENESS`
  override val prettyPrint: String = "NODE UNIQUENESS"
  override val description: String = "nodeUniquenessConstraints"
}

case object RelUniqueConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_UNIQUENESS"
  override val prettyPrint: String = "RELATIONSHIP UNIQUENESS"
  override val description: String = "relationshipUniquenessConstraints"
}

case object ExistsConstraints extends ShowConstraintType {
  override val output: String = "PROPERTY_EXISTENCE"
  override val prettyPrint: String = "PROPERTY EXISTENCE"
  override val description: String = "existenceConstraints"
}

case object NodeExistsConstraints extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_EXISTENCE"
  override val prettyPrint: String = "NODE PROPERTY EXISTENCE"
  override val description: String = "nodeExistenceConstraints"
}

case object RelExistsConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_EXISTENCE"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY EXISTENCE"
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
