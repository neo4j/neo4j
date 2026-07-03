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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.frontend.phases
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlanStringTest.WhiteList
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
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
    subTypes(classOf[expressions.Expression]).foreach { expressionClass =>
      if (!Modifier.isAbstract(expressionClass.getModifiers)) {
        checkStringFields(expressionClass, expressionClass.getName, seen)
      }
    }
  }

  test("IR is not allowed to refer to variables by string") {
    val seen = mutable.Set.empty[Class[_]]
    subTypes(classOf[ir.PlannerQuery]).foreach { irClass =>
      if (!Modifier.isAbstract(irClass.getModifiers)) {
        checkStringFields(irClass, irClass.getName, seen)
      }
    }
  }

  test("logical plans are not allowed to refer to variables by string") {
    val seen = mutable.Set.empty[Class[_]]
    subTypes(classOf[plans.LogicalPlan]).foreach { planClass =>
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
    !classOf[plans.LogicalPlan].isAssignableFrom(method.getReturnType) &&
    !classOf[expressions.Expression].isAssignableFrom(method.getReturnType) &&
    !isWhiteListedAccessor(method.getDeclaringClass, method.getReturnType, method.getName)
  }

  private def lookInside(field: Field): Boolean = {
    !classOf[plans.LogicalPlan].isAssignableFrom(field.getType) &&
    !classOf[expressions.Expression].isAssignableFrom(field.getType) &&
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
      classOf[expressions.AndedPropertyInequalities] -> "inequalities",
      classOf[ir.PatternRelationship] -> "boundaryNodes",
      classOf[ir.PatternRelationship] -> "inOrder",
      classOf[plans.MultiNodeIndexSeek] -> "copyWithoutGettingValues",
      classOf[plans.AssertingMultiNodeIndexSeek] -> "copyWithoutGettingValues",
      classOf[plans.ProjectingPlan] -> "projectExpressions",
      classOf[plans.AggregatingPlan] -> "groupingExpressions",
      classOf[plans.AggregatingPlan] -> "aggregationExpressions",
      classOf[expressions.FilteringExpression] -> "name",
      classOf[expressions.ShortestPathsPatternPart] -> "name",
      classOf[plans.CopyRolePrivileges] -> "grantDeny",
      classOf[plans.DoNothingIfDatabaseNotExists] -> "operation",
      classOf[plans.DoNothingIfNotExists] -> "operation",
      classOf[plans.AdministrationCommandLogicalPlan] -> "command",
      classOf[plans.SecurityAdministrationLogicalPlan] -> "valueMapper",
      classOf[plans.AdministrationCommandLogicalPlan] -> "action",
      classOf[plans.EnsureNodeExists] -> "extraFilter",
      classOf[plans.EnsureNodeExists] -> "labelDescription",
      classOf[plans.EnsureDatabaseNodeExists] -> "extraFilter",
      classOf[plans.EnsureRoleHasNoDeniedPrivileges] -> "subquery",
      classOf[plans.EnsureRoleNotGrantedToAnyAuthRules] -> "subquery",
      classOf[plans.AllowedNonAdministrationCommands] -> "statement",
      classOf[plans.AdministrationCommandLogicalPlan] -> "revokeType",
      classOf[expressions.Expression] -> "dependencies",
      classOf[expressions.PathStep] -> "dependencies",
      classOf[plans.TriadicBuild] -> "triadicSelectionId",
      classOf[plans.NullifyMetadata] -> "key",
      classOf[ast.CollectExpression] -> "query",
      classOf[expressions.PatternElement] -> "identity",
      classOf[expressions.RelationshipPattern] -> "identity",
      classOf[expressions.NonPrefixedPatternPart] -> "identity",
      classOf[expressions.HasMappableExpressions[_]] -> "identity",
      classOf[plans.RunQueryAt] -> "query",
      classOf[plans.RunQueryAt] -> "graphReference",
      classOf[plans.RunQueryAt] -> "parameters",
      classOf[ir.RunQueryAtProjection] -> "graphReference",
      classOf[ir.RunQueryAtProjection] -> "queryString",
      classOf[ast.GraphFunctionReference] -> "print",
      classOf[ast.GraphDirectReference] -> "print",
      classOf[ast.GraphReference] -> "print",
      classOf[plans.DynamicElement.SetOperator] -> "name",
      classOf[plans.NodeVectorIndexSearch] -> "indexName",
      classOf[plans.DirectedRelationshipVectorIndexSearch] -> "indexName",
      classOf[plans.UndirectedRelationshipVectorIndexSearch] -> "indexName",
      classOf[ir.VectorSearchClause] -> "indexName",
      classOf[plans.NodeFulltextIndexSearch] -> "indexName",
      classOf[plans.DirectedRelationshipFulltextIndexSearch] -> "indexName",
      classOf[plans.UndirectedRelationshipFulltextIndexSearch] -> "indexName"
    )

    val whiteListedClasses: Set[Class[_]] = Set[Class[_]](
      classOf[expressions.LogicalVariable],
      classOf[expressions.PathLengthQuantifier],
      classOf[expressions.FunctionInvocation],
      classOf[ast.semantics.SemanticCheck],
      classOf[plans.PointBoundingBoxSeekRangeWrapper],
      classOf[ast.DatabaseName],
      classOf[plans.ErrorPlan],
      classOf[plans.Prober],
      classOf[plans.SystemProcedureCall],
      classOf[plans.SchemaLogicalPlan],
      classOf[expressions.RelationshipTypeToken],
      classOf[expressions.PropertyKeyToken],
      classOf[expressions.LabelToken],
      classOf[expressions.Literal],
      classOf[expressions.ImplicitProcedureArgument],
      classOf[expressions.ExplicitParameter],
      classOf[expressions.LogicalProperty],
      classOf[CypherType],
      classOf[LabelExpression],
      classOf[expressions.NoneIterablePredicate],
      classOf[expressions.AnyIterablePredicate],
      classOf[expressions.SingleIterablePredicate],
      classOf[expressions.ListComprehension],
      classOf[expressions.PropertyKeyName],
      classOf[expressions.Parameter],
      classOf[plans.InequalitySeekRangeWrapper],
      classOf[phases.ResolvedFunctionInvocation],
      classOf[plans.CommandLogicalPlan],
      classOf[expressions.RelTypeName],
      classOf[expressions.LabelName],
      classOf[phases.ResolvedNonLocalCall],
      classOf[plans.PointDistanceRange[_]],
      classOf[plans.PrefixRange[_]],
      classOf[plans.QueryExpression[_]],
      classOf[ast.Options],
      classOf[ast.DatabaseScope],
      classOf[ast.DropDatabaseAdditionalAction],
      classOf[ast.DropDatabaseAliasAction],
      classOf[ast.WaitUntilComplete],
      classOf[ast.PrivilegeQualifier],
      classOf[ast.PropertyResource],
      classOf[ast.ActionResource],
      classOf[ast.DbmsAction],
      classOf[plans.AssertNotCurrentUser],
      classOf[ast.IsTyped],
      classOf[ast.IsNotTyped],
      classOf[ast.IsNormalized],
      classOf[ast.IsNotNormalized],
      classOf[plans.NFA],
      classOf[Exception],
      classOf[IdentityMap[_, _]],
      classOf[ast.Clause],
      classOf[ast.semantics.CustomExpression],
      classOf[ast.semantics.ErrorExpression],
      classOf[ast.CatalogName],
      classOf[ListSet[_]],
      classOf[plans.AssertSecurityDisabled],
      classOf[plans.AssertNotInvalidActionOnShard],
      classOf[plans.AssertNotShardedDatabase],
      classOf[plans.AssertNotStandard],
      classOf[plans.AssertNotVirtualSpd],
      classOf[plans.AssertNotGraphShard],
      classOf[plans.AssertNotPropertyShard],
      classOf[InputPosition],
      classOf[ast.DummyExpression],
      classOf[Seq[_]],
      classOf[NonEmptyList[_]]
    )

    val whiteListedMethodNames: Set[String] = Set(
      "toString",
      "productElement",
      "productPrefix",
      "productIterator",
      "productElementName",
      "productElementNames",
      "_1",
      "_2",
      "_3",
      "_4",
      "_5",
      "_6",
      "_7",
      "_8",
      "_9",
      "_10",
      "_11",
      "_12",
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
      "solvedStringSuffix",
      "planDescriptionDetails"
    )
  }
}
