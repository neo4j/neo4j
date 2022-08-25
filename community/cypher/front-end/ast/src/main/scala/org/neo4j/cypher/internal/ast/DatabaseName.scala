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

import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

import java.util

import scala.jdk.CollectionConverters.ListHasAsScala

sealed trait DatabaseName extends ASTNode {
  def asLegacyName: Either[String, Parameter]
}

case class NamespacedName(nameComponents: List[String], namespace: Option[String] = None)(val position: InputPosition)
    extends DatabaseName {
  val name: String = nameComponents.mkString(".")

  override def toString: String = (namespace ++ Seq(name)).mkString(".")

  override def asLegacyName: Either[String, Parameter] = Left(toString)
}

object NamespacedName {

  def apply(names: util.List[String])(pos: InputPosition): NamespacedName = names.asScala.toSeq match {
    case x :: Nil => NamespacedName(List(x), None)(pos)
    case x :: xs  => NamespacedName(xs, Some(x))(pos)
  }

  def apply(name: String)(pos: InputPosition): NamespacedName = NamespacedName(List(name), None)(pos)
}

case class ParameterName(parameter: Parameter)(val position: InputPosition) extends DatabaseName {
  override def asLegacyName: Either[String, Parameter] = Right(parameter)
}
