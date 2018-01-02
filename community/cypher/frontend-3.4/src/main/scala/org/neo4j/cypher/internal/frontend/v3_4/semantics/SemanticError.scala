/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.util.v3_4.InputPosition

sealed trait SemanticErrorDef {
  def msg: String
  def position: InputPosition
  def references: Seq[InputPosition]
}

final case class SemanticError(msg: String, position: InputPosition, references: InputPosition*) extends SemanticErrorDef

sealed trait UnsupportedOpenCypher extends SemanticErrorDef

final case class FeatureError(msg: String, position: InputPosition) extends UnsupportedOpenCypher {
  override def references = Seq.empty
}
