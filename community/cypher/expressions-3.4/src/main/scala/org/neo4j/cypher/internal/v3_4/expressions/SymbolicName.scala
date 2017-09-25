/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.aux.v3_4.{ASTNode, InputPosition}

trait SymbolicName extends ASTNode {
  def name: String
  def position: InputPosition
  override def asCanonicalStringVal: String = name
}

case class Namespace(parts: List[String] = List.empty)(val position: InputPosition) extends ASTNode

case class ProcedureName(name: String)(val position: InputPosition) extends ASTNode with SymbolicName

case class ProcedureOutput(name: String)(val position: InputPosition) extends ASTNode with SymbolicName

case class LabelName(name: String)(val position: InputPosition) extends SymbolicName

case class PropertyKeyName(name: String)(val position: InputPosition) extends SymbolicName

case class RelTypeName(name: String)(val position: InputPosition) extends SymbolicName
