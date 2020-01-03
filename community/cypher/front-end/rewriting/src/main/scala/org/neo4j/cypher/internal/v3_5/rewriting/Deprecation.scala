/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util._

import scala.collection.immutable.TreeMap

object Deprecations {

  def propertyOf(propertyKey:String): Expression => Expression =
    e => Property(e, PropertyKeyName(propertyKey)(e.position))(e.position)

  def renameFunctionTo(newName: String): FunctionInvocation => FunctionInvocation =
    f => f.copy(functionName = FunctionName(newName)(f.functionName.position), deprecated = true)(f.position)

  case object V1 extends Deprecations {
    private val functionRenames: Map[String, String] =
      TreeMap(
        "toInt" -> "toInteger",
        "upper" -> "toUpper",
        "lower" -> "toLower",
        "rels" -> "relationships"
      )(CaseInsensitiveOrdered)

    override val find: PartialFunction[Any, Deprecation] = {

      // straight renames
      case f@FunctionInvocation(namespace, FunctionName(name), distinct, args,_) if functionRenames.contains(name) =>
        Deprecation(
          () => renameFunctionTo(functionRenames(name))(f),
          () => Some(DeprecatedFunctionNotification(f.position, name, functionRenames(name)))
        )

      // timestamp
      case f@FunctionInvocation(namespace, FunctionName(name), distinct, args,_) if name.equalsIgnoreCase("timestamp")=>
        Deprecation(
          () => renameFunctionTo("datetime").andThen(propertyOf("epochMillis"))(f),
          () => None
        )

      // var-length binding
      case p@RelationshipPattern(Some(variable), _, Some(_), _, _, _, _) =>
        Deprecation(
          () => p,
          () => Some(DeprecatedVarLengthBindingNotification(p.position, variable.name))
        )

      // legacy type separator
      case p@RelationshipPattern(variable, _, length, properties, _, true, _) if variable.isDefined || length.isDefined || properties.isDefined =>
        Deprecation(
          () => p,
          () => Some(DeprecatedRelTypeSeparatorNotification(p.position))
        )

      // old parameter syntax
      case p@ParameterWithOldSyntax(name, parameterType) =>
        Deprecation(
          () => Parameter(name, parameterType)(p.position),
          () => Some(DeprecatedParameterSyntax(p.position))
        )
    }
  }

  case object V2 extends Deprecations {
    private val functionRenames: Map[String, String] =
      TreeMap(
        "toInt" -> "toInteger",
        "upper" -> "toUpper",
        "lower" -> "toLower",
        "rels" -> "relationships"
      )(CaseInsensitiveOrdered)

    override val find: PartialFunction[Any, Deprecation] = {

      val additionalDeprecations: PartialFunction[Any, Deprecation] = {

        // extract => list comprehension
        case e@ExtractExpression(scope, expression) =>
          Deprecation(
            () => ListComprehension(scope, expression, generatedThroughRewrite = true)(e.position),
            () => Some(DeprecatedFunctionNotification(e.position, "extract(...)", "[...]"))
          )

        // filter => list comprehension
        case e@FilterExpression(scope, expression) =>
          Deprecation(
            () => ListComprehension(ExtractScope(scope.variable, scope.innerPredicate, None)(scope.position), expression, generatedThroughRewrite = true)(e.position),
            () => Some(DeprecatedFunctionNotification(e.position, "filter(...)", "[...]"))
          )

      }

      additionalDeprecations.orElse(V1.find)
    }
  }

  object CaseInsensitiveOrdered extends Ordering[String] {
    def compare(x: String, y: String): Int =
      x.compareToIgnoreCase(y)
  }
}

/**
  * One deprecation.
  *
  * This class holds both the ability to replace a part of the AST with the preferred non-deprecated variant, and
  * the ability to generate an optional notification to the user that they are using a deprecated feature.
  *
  * @param generateReplacement function which rewrites the matched construct
  * @param generateNotification function which generates an appropriate deprecation notification
  */
case class Deprecation(generateReplacement: () => ASTNode, generateNotification: () => Option[InternalNotification])

trait Deprecations extends {
  def find: PartialFunction[Any, Deprecation]
}
