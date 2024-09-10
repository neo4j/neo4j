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

import org.neo4j.cypher.internal.util.InputPosition

sealed trait ActionResourceBase {
  def simplify: Seq[ActionResource]
}

sealed trait ActionResource extends ActionResourceBase {
  override def simplify: Seq[ActionResource] = Seq(this)
}

sealed trait NestedActionResource extends ActionResourceBase {}

final case class PropertyResource(property: String)(val position: InputPosition) extends ActionResource

final case class PropertiesResource(properties: Seq[String])(val position: InputPosition) extends NestedActionResource {
  override def simplify: Seq[ActionResource] = properties.map(PropertyResource(_)(position))
}

final case class AllPropertyResource()(val position: InputPosition) extends ActionResource

final case class LabelResource(label: String)(val position: InputPosition) extends ActionResource

final case class LabelsResource(labels: Seq[String])(val position: InputPosition) extends NestedActionResource {
  override def simplify: Seq[ActionResource] = labels.map(LabelResource(_)(position))
}

final case class AllLabelResource()(val position: InputPosition) extends ActionResource

final case class NoResource()(val position: InputPosition) extends ActionResource

final case class DatabaseResource()(val position: InputPosition) extends ActionResource

final case class FileResource()(val position: InputPosition) extends ActionResource
