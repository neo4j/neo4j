/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PropertyResource
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilteringExpression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.RunQueryAtHorizon
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlanStringTest.WhiteList
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.reflections.Reflections

import java.lang.reflect.Field
import java.lang.reflect.Method
import java.lang.reflect.Modifier

import scala.collection.mutable
import scala.jdk.CollectionConverters.SetHasAsScala

/**
 * Tries to make sure we don't introduce variable references as strings in logical plans.
 * This implementation is far from complete, for example it ignores everything with generic types like collections.
 */
class LogicalPlanStringTest extends CypherFunSuite {
  private val reflections = new Reflections("org.neo4j")

  test("expressions are not allowed to refer to variables by string") {
    val seen = mutable.Set.empty[Class[_]]
    subTypes(classOf[Expression]).foreach { expressionClass =>
      if (!Modifier.isAbstract(expressionClass.getModifiers)) {
        checkStringFields(expressionClass, expressionClass.getName, seen)
      }
    }
  }

  test("IR is not allowed to refer to variables by string") {
    val seen = mutable.Set.empty[Class[_]]
    subTypes(classOf[PlannerQuery]).foreach { irClass =>
      if (!Modifier.isAbstract(irClass.getModifiers)) {
        checkStringFields(irClass, irClass.getName, seen)
      }
    }
  }

  test("logical plans are not allowed to refer to variables by string") {
    val seen = mutable.Set.empty[Class[_]]
    subTypes(classOf[LogicalPlan]).foreach { planClass =>
      if (!Modifier.isAbstract(planClass.getModifiers)) {
        checkStringFields(planClass, planClass.getName, seen)
      }
    }
  }

  test("accessor white list is correct") {
    WhiteList.whiteListedAccessors.foreach { case (cls, accessor) =>
      withClue(s"${cls.getName} do not have accessor $accessor\n") {
        def hasField(cls: Class[_]) = cls.getFields.exists(f => f.getName == accessor)
        def hasMethod(cls: Class[_]) = cls.getMethods.exists(f => f.getName == accessor)
        def subTypeHasFieldOrMethod = subTypes(cls).exists(c => hasField(c) || hasMethod(c))
        assert(hasField(cls) || hasMethod(cls) || subTypeHasFieldOrMethod)
      }
    }
  }

  private def checkStringFields(
    cls: Class[_],
    path: String,
    seen: mutable.Set[Class[_]]
  ): Unit = {
    if (seen.add(cls) && !isWhiteListedClass(cls)) {
      if (mightBeVariableAsString(cls)) {
        fail(
          s"""
             |Hello! Yes you.
             |
             |I found a path in an expression or in IR or a logical plan that might(!) reference a variable using a string:
             |
             |$path: ${cls.getSimpleName}
             |
             |It's of great importance that we don't reference variables by strings (org.neo4j.cypher.internal.physicalplanning.LivenessAnalysis relies on it).
             |
             |You need to take one of the following actions:
             |
             |- You did use a String to reference a variable.
             |    1. Change it to LogicalVariable, test should now pass.
             |    2. You may need to update SlottedRewriter if the variable does not get rewritten:
             |       - If the variable is allocated in the incoming slot configuration you may need to add a case for it.
             |       - If the variable should use an expression variable you may need to add a case for it in expressionVariableAllocation.
             |       - If the variable should not allocate a slot or an expression variable for some reason,
             |         you may need to add a case where you rewrite it to a VariableRef.
             |
             |- You did not use a String to reference a variable.
             |    1. Modify one of the white lists in object WhiteList.
             |       Try to be specific to keep the test working for future plans.
             |
             |Sorry for the inconvenience! Have a nice day 🌞.
             |""".stripMargin.trim
        )
      }

      subTypes(cls).foreach { subType =>
        checkStringFields(subType, s"$path(${subType.getSimpleName})", seen)
      }
      cls.getFields.foreach { field =>
        if (lookInside(field)) {
          checkStringFields(field.getType, s"$path.${field.getName}", seen)
        }
      }
      cls.getMethods.foreach { method =>
        if (lookInside(method)) {
          checkStringFields(method.getReturnType, s"$path.${method.getName}()", seen)
        }
      }
    }
  }

  private def mightBeVariableAsString(cls: Class[_]): Boolean = {
    cls.isAssignableFrom(classOf[String])
  }

  private def lookInside(method: Method): Boolean = {
    method.getParameterCount == 0 &&
    method.getDeclaringClass.getName.startsWith("org.neo4j") &&
    !Modifier.isStatic(method.getModifiers) &&
    !classOf[LogicalPlan].isAssignableFrom(method.getReturnType) &&
    !classOf[Expression].isAssignableFrom(method.getReturnType) &&
    !isWhiteListedAccessor(method.getDeclaringClass, method.getReturnType, method.getName)
  }

  private def lookInside(field: Field): Boolean = {
    !classOf[LogicalPlan].isAssignableFrom(field.getType) &&
    !classOf[Expression].isAssignableFrom(field.getType) &&
    !isWhiteListedAccessor(field.getDeclaringClass, field.getType, field.getName)
  }

  private def isWhiteListedAccessor(declaringCls: Class[_], returnCls: Class[_], name: String): Boolean = {
    name.contains("$") ||
    isWhiteListedName(name) ||
    isWhiteListedClass(returnCls) ||
    WhiteList.whiteListedAccessors.exists {
      case (whiteListClass, whiteListName) =>
        whiteListClass.isAssignableFrom(declaringCls) && whiteListName == name
    }
  }

  private def isWhiteListedName(name: String) = {
    WhiteList.whiteListedMethodNames.contains(name)
  }

  private def isWhiteListedClass(cls: Class[_]) = {
    WhiteList.whiteListedClasses.contains(cls) ||
    WhiteList.whiteListedClasses.exists { whiteListedClass =>
      whiteListedClass.isAssignableFrom(cls)
    }
  }

  private def subTypes(cls: Class[_]): Iterable[Class[_]] = {
    reflections.getSubTypesOf(cls).asScala
  }
}

object LogicalPlanStringTest {

  object WhiteList {

    val whiteListedAccessors: Set[(Class[_], String)] = Set[(Class[_], String)](
      classOf[AndedPropertyInequalities] -> "inequalities",
      classOf[PatternRelationship] -> "boundaryNodes",
      classOf[PatternRelationship] -> "inOrder",
      classOf[MultiNodeIndexSeek] -> "copyWithoutGettingValues",
      classOf[AssertingMultiNodeIndexSeek] -> "copyWithoutGettingValues",
      classOf[ProjectingPlan] -> "projectExpressions",
      classOf[AggregatingPlan] -> "groupingExpressions",
      classOf[AggregatingPlan] -> "aggregationExpressions",
      classOf[FilteringExpression] -> "name",
      classOf[ShortestPathsPatternPart] -> "name",
      classOf[CopyRolePrivileges] -> "grantDeny",
      classOf[DoNothingIfDatabaseNotExists] -> "operation",
      classOf[DoNothingIfNotExists] -> "operation",
      classOf[AdministrationCommandLogicalPlan] -> "command",
      classOf[SecurityAdministrationLogicalPlan] -> "label",
      classOf[SecurityAdministrationLogicalPlan] -> "valueMapper",
      classOf[AdministrationCommandLogicalPlan] -> "action",
      classOf[EnsureNodeExists] -> "extraFilter",
      classOf[EnsureNodeExists] -> "labelDescription",
      classOf[EnsureDatabaseNodeExists] -> "extraFilter",
      classOf[AllowedNonAdministrationCommands] -> "statement",
      classOf[AdministrationCommandLogicalPlan] -> "revokeType",
      classOf[Expression] -> "dependencies",
      classOf[PathStep] -> "dependencies",
      classOf[TriadicBuild] -> "triadicSelectionId",
      classOf[NullifyMetadata] -> "key",
      classOf[CollectExpression] -> "query",
      classOf[PatternElement] -> "identity",
      classOf[RelationshipPattern] -> "identity",
      classOf[NonPrefixedPatternPart] -> "identity",
      classOf[HasMappableExpressions[_]] -> "identity",
      classOf[RunQueryAt] -> "query",
      classOf[RunQueryAt] -> "graphReference",
      classOf[RunQueryAt] -> "parameters",
      classOf[RunQueryAtHorizon] -> "graphReference",
      classOf[RunQueryAtHorizon] -> "queryString"
    )

    val whiteListedClasses: Set[Class[_]] = Set[Class[_]](
      classOf[LogicalVariable],
      classOf[FunctionInvocation],
      classOf[SemanticCheck],
      classOf[PointBoundingBoxSeekRangeWrapper],
      classOf[DatabaseName],
      classOf[ErrorPlan],
      classOf[Prober],
      classOf[SystemProcedureCall],
      classOf[SchemaLogicalPlan],
      classOf[RelationshipTypeToken],
      classOf[PropertyKeyToken],
      classOf[LabelToken],
      classOf[org.neo4j.cypher.internal.expressions.Literal],
      classOf[ImplicitProcedureArgument],
      classOf[ExplicitParameter],
      classOf[LogicalProperty],
      classOf[CypherType],
      classOf[LabelExpression],
      classOf[NoneIterablePredicate],
      classOf[AnyIterablePredicate],
      classOf[SingleIterablePredicate],
      classOf[ListComprehension],
      classOf[PropertyKeyName],
      classOf[Parameter],
      classOf[InequalitySeekRangeWrapper],
      classOf[ResolvedFunctionInvocation],
      classOf[CommandLogicalPlan],
      classOf[RelTypeName],
      classOf[LabelName],
      classOf[ResolvedCall],
      classOf[PointDistanceRange[_]],
      classOf[PrefixRange[_]],
      classOf[QueryExpression[_]],
      classOf[Options],
      classOf[DatabaseScope],
      classOf[DropDatabaseAdditionalAction],
      classOf[WaitUntilComplete],
      classOf[PrivilegeQualifier],
      classOf[PropertyResource],
      classOf[ActionResource],
      classOf[DbmsAction],
      classOf[AssertNotCurrentUser],
      classOf[IsTyped],
      classOf[IsNotTyped],
      classOf[NFA],
      classOf[Exception],
      classOf[IdentityMap[_, _]],
      classOf[Clause]
    )

    val whiteListedMethodNames: Set[String] = Set(
      "toString",
      "productElement",
      "productPrefix",
      "productIterator",
      "productElementName",
      "productElementNames",
      "dup",
      "verboseToString",
      "foldedOver",
      "folder",
      "debugId",
      "solvedExpressionAsString",
      "asCanonicalStringVal",
      "DefaultTypeMismatchMessageGenerator",
      "canonicalOperatorSymbol",
      "leftArrowCanonicalString",
      "rightArrowCanonicalString",
      "subqueryAstNode",
      "prettified",
      "mkString",
      "className",
      "solvedString",
      "solvedStringSuffix"
    )
  }
}
