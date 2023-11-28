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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.FunctionWithName
import org.neo4j.cypher.internal.util.symbols.CypherType

trait TypeSignature {

  def argumentTypes: IndexedSeq[CypherType]

  def outputType: CypherType

  def removeFirstArgumentType: TypeSignature

  def getSignatureAsString: String
}

case class FunctionTypeSignature(
  function: FunctionWithName,
  outputType: CypherType,
  names: IndexedSeq[String],
  description: String,
  category: String,
  argumentTypes: IndexedSeq[CypherType],
  optionalTypes: IndexedSeq[CypherType] = Vector.empty,
  deprecated: Boolean = false,
  internal: Boolean = false,
  overrideDefaultAsString: Option[String] = None
) extends TypeSignature {

  override def getSignatureAsString: String = {
    overrideDefaultAsString.getOrElse {
      function.name + "(" + names.zip(
        argumentTypes.map(_.normalizedCypherTypeString()) ++ optionalTypes.map(_.normalizedCypherTypeString())
      ).map { f =>
        f._1 + " :: " + f._2
      }.mkString(", ") + ") :: " + outputType.normalizedCypherTypeString()
    }
  }

  override def removeFirstArgumentType: TypeSignature = this.copy(argumentTypes = this.argumentTypes.tail)

  def isAggregationFunction: Boolean = function match {
    case _: functions.AggregatingFunction => true
    case _                                => false
  }
}

object TypeSignature {

  def deprecated(
    function: Function,
    argumentType: CypherType,
    outputType: CypherType,
    description: String,
    category: String
  ) =
    FunctionTypeSignature(
      function,
      outputType,
      Vector("input"),
      description,
      category,
      Vector(argumentType),
      deprecated = true
    )

  def apply(
    function: Function,
    argumentType: CypherType,
    outputType: CypherType,
    description: String,
    category: String
  ): FunctionTypeSignature =
    FunctionTypeSignature(function, outputType, Vector("input"), description, category, Vector(argumentType))

  def noArg(function: Function, outputType: CypherType, description: String, category: String): FunctionTypeSignature =
    FunctionTypeSignature(function, outputType, Vector("input"), description, category, Vector())

  def apply(argumentTypes: IndexedSeq[CypherType], outputType: CypherType) =
    ExpressionTypeSignature(argumentTypes, outputType)
}

case class ExpressionTypeSignature(argumentTypes: IndexedSeq[CypherType], outputType: CypherType)
    extends TypeSignature {
  override def removeFirstArgumentType: TypeSignature = this.copy(argumentTypes = this.argumentTypes.tail)

  def getSignatureAsString: String =
    argumentTypes.map(_.normalizedCypherTypeString()).mkString(
      ", "
    ) ++ ") :: " + outputType.normalizedCypherTypeString()
}
