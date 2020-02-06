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
package org.neo4j.cypher.internal.v4_0.rewriting

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util._

import scala.collection.immutable.TreeMap

object Deprecations {

  def propertyOf(propertyKey:String): Expression => Expression =
    e => Property(e, PropertyKeyName(propertyKey)(e.position))(e.position)

  def renameFunctionTo(newName: String): FunctionInvocation => FunctionInvocation =
    f => f.copy(functionName = FunctionName(newName)(f.functionName.position))(f.position)

  case object V1 extends Deprecations {
    val functionRenames: Map[String, String] =
      TreeMap(
        // put any V1 deprecation here as
        // "old name" -> "new name"
      )(CaseInsensitiveOrdered)

    override val find: PartialFunction[Any, Deprecation] = {

      // straight renames
      case f@FunctionInvocation(namespace, FunctionName(name), distinct, args) if functionRenames.contains(name) =>
        Deprecation(
          () => renameFunctionTo(functionRenames(name))(f),
          () => Some(DeprecatedFunctionNotification(f.position, name, functionRenames(name)))
        )

      // timestamp
      case f@FunctionInvocation(namespace, FunctionName(name), distinct, args) if name.equalsIgnoreCase("timestamp")=>
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

      case i: ast.CreateIndex =>
        Deprecation(
          () => i,
          () => Some(DeprecatedCreateIndexSyntax(i.position))
        )

      case i: ast.DropIndex =>
        Deprecation(
          () => i,
          () => Some(DeprecatedDropIndexSyntax(i.position))
        )

      case c: ast.DropNodeKeyConstraint =>
        Deprecation(
          () => c,
          () => Some(DeprecatedDropConstraintSyntax(c.position))
        )

      case c: ast.DropUniquePropertyConstraint =>
        Deprecation(
          () => c,
          () => Some(DeprecatedDropConstraintSyntax(c.position))
        )

      case c: ast.DropNodePropertyExistenceConstraint =>
        Deprecation(
          () => c,
          () => Some(DeprecatedDropConstraintSyntax(c.position))
        )

      case c: ast.DropRelationshipPropertyExistenceConstraint =>
        Deprecation(
          () => c,
          () => Some(DeprecatedDropConstraintSyntax(c.position))
        )
    }
  }

  case object V2 extends Deprecations {
    private val functionRenames: Map[String, String] =
      TreeMap(
        // put any V2 deprecation here as
        // "old name" -> "new name"
      )(CaseInsensitiveOrdered)

    override val find: PartialFunction[Any, Deprecation] = V1.find
  }

  // This is functionality that have been removed in 4.0 but still should work (but be deprecated) when using CYPHER 3.5
  case object removedFeaturesIn4_0 extends Deprecations {
    val removedFunctionsRenames: Map[String, String] =
      TreeMap(
        "toInt" -> "toInteger",
        "upper" -> "toUpper",
        "lower" -> "toLower",
        "rels" -> "relationships"
      )(CaseInsensitiveOrdered)

    override def find: PartialFunction[Any, Deprecation] = {

      case f@FunctionInvocation(_, FunctionName(name), _, _) if removedFunctionsRenames.contains(name) =>
        Deprecation(
          () => renameFunctionTo(removedFunctionsRenames(name))(f),
          () => Some(DeprecatedFunctionNotification(f.position, name, removedFunctionsRenames(name)))
        )

      // extract => list comprehension
      case e@ExtractExpression(scope, expression) =>
        Deprecation(
          () => ListComprehension(scope, expression)(e.position),
          () => Some(DeprecatedFunctionNotification(e.position, "extract(...)", "[...]"))
        )

      // filter => list comprehension
      case e@FilterExpression(scope, expression) =>
        Deprecation(
          () => ListComprehension(ExtractScope(scope.variable, scope.innerPredicate, None)(scope.position), expression)(e.position),
          () => Some(DeprecatedFunctionNotification(e.position, "filter(...)", "[...]"))
        )

      // old parameter syntax
      case p@ParameterWithOldSyntax(name, parameterType) =>
        Deprecation(
          () => Parameter(name, parameterType)(p.position),
          () => Some(DeprecatedParameterSyntax(p.position))
        )

      // length of a string, collection or pattern expression
      case f@FunctionInvocation(_, FunctionName(name), _, args)
        if name.toLowerCase.equals("length") && args.nonEmpty &&
          (args.head.isInstanceOf[StringLiteral] || args.head.isInstanceOf[ListLiteral] || args.head.isInstanceOf[PatternExpression]) =>
        Deprecation(
          () => renameFunctionTo("size")(f),
          () => Some(LengthOnNonPathNotification(f.position))
        )

      // legacy type separator
      case p@RelationshipPattern(variable, _, length, properties, _, true, _) if variable.isDefined || length.isDefined || properties.isDefined =>
        Deprecation(
          () => p.copy(legacyTypeSeparator = false)(p.position),
          () => Some(DeprecatedRelTypeSeparatorNotification(p.position))
        )
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
