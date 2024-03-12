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

import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.MapValue

import java.util

import scala.jdk.CollectionConverters.ListHasAsScala

sealed trait DatabaseName extends ASTNode {
  def asLegacyName: Either[String, Parameter]
}

case class NamespacedName(nameComponents: List[String], namespace: Option[String])(val position: InputPosition)
    extends DatabaseName {
  val name: String = nameComponents.mkString(".")

  override def toString: String = (namespace ++ Seq(name)).mkString(".")

  override def asLegacyName: Either[String, Parameter] = Left(toString)
}

object NamespacedName {

  def apply(names: util.List[String])(pos: InputPosition): NamespacedName = apply(names.asScala.toList)(pos)

  def apply(names: List[String])(pos: InputPosition): NamespacedName = names match {
    case x :: Nil => NamespacedName(List(x), None)(pos)
    case x :: xs  => NamespacedName(xs, Some(x))(pos)
    case _        => throw new InternalError(s"Unexpected database name format")
  }
  def apply(name: String)(pos: InputPosition): NamespacedName = NamespacedName(List(name), None)(pos)
}

case class ParameterName(parameter: Parameter)(val position: InputPosition) extends DatabaseName {
  override def asLegacyName: Either[String, Parameter] = Right(parameter)

  def getNameParts(params: MapValue, defaultNamespace: String): (Option[String], String, String) = {
    val paramValue = params.get(parameter.name)
    if (!paramValue.isInstanceOf[TextValue]) {
      throw new ParameterWrongTypeException(
        s"Expected parameter $$${parameter.name} to have type String but was $paramValue"
      )
    } else {
      val nameParts = paramValue.asInstanceOf[TextValue].stringValue().split('.')
      if (nameParts.length == 1) {
        (None, nameParts(0), nameParts(0))
      } else {
        val displayName =
          if (nameParts(0).equals(defaultNamespace))
            nameParts.tail.mkString(".")
          else paramValue.asInstanceOf[TextValue].stringValue()
        (Some(nameParts(0)), nameParts.tail.mkString("."), displayName)
      }
    }
  }
}
