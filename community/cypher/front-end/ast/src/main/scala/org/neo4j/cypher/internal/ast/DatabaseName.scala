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

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.MapValue

import java.util

import scala.jdk.CollectionConverters.ListHasAsScala

sealed trait DatabaseName extends ASTNode {
  def asLegacyName: Either[String, Parameter]
}

object DatabaseName {

  def apply(either: Either[String, Parameter])(pos: InputPosition): DatabaseName = either match {
    case Left(name)   => NamespacedName(name)(pos)
    case Right(param) => ParameterName(param)(pos)
  }
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

case class ParameterName(expression: Expression)(val position: InputPosition) extends DatabaseName {
  private val parameterLazy: LazyVal[Parameter] = LazyVal(expression.asInstanceOf[Parameter])
  def parameter: Parameter = parameterLazy.value

  // This will not work if the parameter has been slotted (ParameterFromSlot), but that is only used for SHOW DATABASES
  // and in that case we will not use this method
  override def asLegacyName: Either[String, Parameter] = Right(parameter)

  def getNameParts(
    params: ParameterProvider,
    defaultNamespace: String,
    emulateGetNameFields: Boolean = false,
    allowAndPassThroughNullInput: Boolean = false
  ): (Option[String], String, String, String) = {
    val paramValue = params.get(expression)
    if (allowAndPassThroughNullInput && paramValue.isInstanceOf[NoValue]) {
      (None, null, null, null)
    } else if (!paramValue.isInstanceOf[TextValue]) {
      throw ParameterWrongTypeException.expectedParameterToBeString42N51(
        false,
        // On the SHOW DATABASE path, we might be using slotted parameters
        params.getName(expression),
        String.valueOf(paramValue),
        paramValue.prettify()
      )
    } else {
      def backtick(s: String) = ExpressionStringifier().backtick(s)
      val paramStringValue = paramValue.asInstanceOf[TextValue].stringValue()
      val namePartsSplit = paramStringValue.split('.')
      // To not lose trailing dots we add in the empty string that followed it
      val nameParts = if (paramStringValue.endsWith(".")) namePartsSplit :+ "" else namePartsSplit
      if (nameParts.length == 1) {
        (None, nameParts(0), nameParts(0), backtick(nameParts(0)))
      } else if (emulateGetNameFields) {
        val displayName = paramValue.asInstanceOf[TextValue].stringValue()
        val quotedDisplayName = backtick(displayName)
        (None, displayName, displayName, quotedDisplayName)
      } else {
        val displayName =
          if (nameParts(0).equals(defaultNamespace))
            nameParts.tail.mkString(".")
          else paramStringValue
        val quotedDisplayName = {
          val name = backtick(nameParts.tail.mkString("."))
          if (nameParts(0).equals(defaultNamespace)) name
          else s"${backtick(nameParts(0))}.$name"
        }
        (Some(nameParts(0)), nameParts.tail.mkString("."), displayName, quotedDisplayName)
      }
    }
  }
}

trait ParameterProvider {
  val get: PartialFunction[Expression, AnyValue]
  val getName: PartialFunction[Expression, String]
}

case class MapBasedParameterProvider(mapValue: MapValue) extends ParameterProvider {

  override val get: PartialFunction[Expression, AnyValue] = {
    case p: Parameter => mapValue.get(p.name)
  }

  override val getName: PartialFunction[Expression, String] = {
    case p: Parameter => p.name
  }
}
