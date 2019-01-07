/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_5

import java.lang.reflect.Constructor

import org.neo4j.cypher.internal.compatibility.v3_5.SemanticTableConverter.ExpressionMapping4To5
import org.neo4j.cypher.internal.ir.{v3_5 => irV3_5, v4_0 => irV4_0}
import org.neo4j.cypher.internal.planner.v3_5.spi.{PlanningAttributes => PlanningAttributesV3_5}
import org.neo4j.cypher.internal.planner.v4_0.spi.{PlanningAttributes => PlanningAttributesV4_0}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan => LogicalPlanV3_5}
import org.neo4j.cypher.internal.v3_5.logical.{plans => plansV3_5}
import org.neo4j.cypher.internal.v4_0.expressions.{InvalidNodePattern, LogicalVariable, Expression => ExpressionV4_0}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan => LogicalPlanV4_0}
import org.neo4j.cypher.internal.v4_0.logical.{plans => plansv4_0}
import org.neo4j.cypher.internal.v4_0.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen
import org.neo4j.cypher.internal.v4_0.util.{symbols => symbolsV4_0, _}
import org.neo4j.cypher.internal.v4_0.{expressions => expressionsV4_0, util => utilV4_0}
import org.neo4j.cypher.internal.v3_5.expressions.{Expression => ExpressionV3_5, SemanticDirection => SemanticDirectionV3_5}
import org.neo4j.cypher.internal.v3_5.util.{InputPosition => InputPositionV3_5, symbols => symbolsV3_5}
import org.neo4j.cypher.internal.v3_5.{ast => astV3_5, expressions => expressionsV3_5, util => utilV3_5}

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.{Failure, Success, Try}

/**
  * This is responsible for converting logical plans from the old version to the current version.
  */
object LogicalPlanConverter {

  type MutableExpressionMapping3To4 = mutable.Map[(ExpressionV3_5, InputPositionV3_5), ExpressionV4_0]

  val oldLogicalPlanPackage = "org.neo4j.cypher.internal.v3_5.logical.plans"
  val newLogicalPlanPackage = "org.neo4j.cypher.internal.v4_0.logical.plans"
  val oldASTPackage = "org.neo4j.cypher.internal.v3_5.ast"
  val newASTPackage = "org.neo4j.cypher.internal.v4_0.ast"
  val oldExpressionPackage = "org.neo4j.cypher.internal.v3_5.expressions"
  val newExpressionPackage = "org.neo4j.cypher.internal.v4_0.expressions"
  val oldUtilPackage = "org.neo4j.cypher.internal.v3_5.util"
  val newUtilPackage = "org.neo4j.cypher.internal.v4_0.util"
  val oldIRPackage = "org.neo4j.cypher.internal.ir.v3_5"
  val newIRPackage = "org.neo4j.cypher.internal.ir.v4_0"

  /**
    * This rewriter traverses the tree bottom up and applies the mappings given here. We need to use RewriterWithArgs,
    * since we're changing types while going up, and not yet converted old parent plan cannot hold already converted
    * child plans. We get the already converted child plans as `children: Seq[AnyRef]}` instead.
    *
    * @param expressionMap
    *                      During the tree traversal of logical plans we need to remember which old expressions we
    *                      converted to which new expressions. This is because the new semantic table must have the
    *                      same objects (object identity) as keys, and thus we can't simply convert the old
    *                      expressions again when we convert the semantic table.
    * @param seenBySemanticTable
    *                    Not all expressions have been _seen_ by the semantic table. An expression is not important
    *                    if it has not been seen. Then we won't stick it in the expressionMap
    */
  //noinspection ZeroIndexToHead
  private class LogicalPlanRewriter(planningAttributes3_5: PlanningAttributesV3_5,
                                    planningAttributes4_0: PlanningAttributesV4_0,
                                    ids: IdConverter,
                                    val expressionMap: MutableExpressionMapping3To4 = new mutable.HashMap[(ExpressionV3_5, InputPositionV3_5), ExpressionV4_0],
                                    val seenBySemanticTable: ExpressionV3_5 => Boolean = _ => true)
    extends RewriterWithArgs {

    override def apply(v1: (AnyRef, Seq[AnyRef])): AnyRef = rewriter.apply(v1)

    private val rewriter: RewriterWithArgs = bottomUpWithArgs { before =>
      val rewritten = RewriterWithArgs.lift {
        case (_: plansV3_5.UserFunctionSignature , children) =>
          plansv4_0.UserFunctionSignature(
            children(0).asInstanceOf[plansv4_0.QualifiedName],
            children(1).asInstanceOf[IndexedSeq[plansv4_0.FieldSignature]],
            children(2).asInstanceOf[symbolsV4_0.CypherType],
            children(3).asInstanceOf[Option[String]],
            children(4).asInstanceOf[Array[String]],
            children(5).asInstanceOf[Option[String]],
            children(6).asInstanceOf[Boolean],
            children(7).asInstanceOf[Option[Int]],
            threadSafe = false)

        case ( plan:plansV3_5.ActiveRead, children: Seq[AnyRef]) =>

          // ---------------- PLAN OPERATORS ----------------
          // Can simply be replaced by its child
          children(0).asInstanceOf[LogicalPlanV4_0]

        // ---------------- FALLTHROUGH FOR ALL PLANS ----------------
        case (plan: plansV3_5.LogicalPlan, children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, plan, children, ids.convertId(plan), classOf[IdGen])

        // ---------------- AST NODES ----------------
        case (item: astV3_5.ProcedureResultItem, children: Seq[AnyRef]) =>
          convertVersion(oldASTPackage, newASTPackage, item, children, helpers.as4_0(item.position), classOf[InputPosition])

        // Expressions in the logical plans package, with extra InputPosition argument
        case (item@(_: plansV3_5.PrefixSeekRangeWrapper |
                    _: plansV3_5.InequalitySeekRangeWrapper |
                    _: plansV3_5.PointDistanceSeekRangeWrapper |
                    _: plansV3_5.NestedPlanExpression |
                    _: plansV3_5.ResolvedFunctionInvocation |
                    _: plansV3_5.ResolvedCall |
                    _: plansV3_5.CachedNodeProperty), children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children, helpers.as4_0(item.asInstanceOf[utilV3_5.ASTNode].position), classOf[InputPosition])

        // Expressions in the logical plans package, no extra argument
        case (_: plansV3_5.CoerceToPredicate, children: Seq[AnyRef]) =>
          plansv4_0.CoerceToPredicate(children(0).asInstanceOf[expressionsV4_0.Expression])

        // Expressions in the expressions package
        case (item: expressionsV3_5.PatternComprehension, children: Seq[AnyRef]) =>
          expressionsV4_0.PatternComprehension(
            children(0).asInstanceOf[Option[LogicalVariable]],
            children(1).asInstanceOf[expressionsV4_0.RelationshipsPattern],
            children(2).asInstanceOf[Option[expressionsV4_0.Expression]],
            children(3).asInstanceOf[expressionsV4_0.Expression]
          )(helpers.as4_0(item.position), item.outerScope.map(v => expressionsV4_0.Variable(v.name)(helpers.as4_0(v.position))))

        case (item: expressionsV3_5.MapProjection, children: Seq[AnyRef]) =>
          expressionsV4_0.MapProjection(
            children(0).asInstanceOf[expressionsV4_0.Variable],
            children(1).asInstanceOf[Seq[expressionsV4_0.MapProjectionElement]]
          )(helpers.as4_0(item.position), item.definitionPos.map(helpers.as4_0))

        case (item: expressionsV3_5.InvalidNodePattern, children: Seq[AnyRef]) =>
          new InvalidNodePattern(children(0).asInstanceOf[Option[LogicalVariable]].get)(helpers.as4_0(item.position))

        // ---------------- FALLTHROUGH FOR ALL AST NODES ----------------
        case (expressionV3_5: utilV3_5.ASTNode, children: Seq[AnyRef]) =>
          convertVersion(oldExpressionPackage, newExpressionPackage, expressionV3_5, children, helpers.as4_0(expressionV3_5.position), classOf[InputPosition])

        // ---------------- CASE OBJECTS ----------------
        case (symbolsV3_5.CTAny, _) => symbolsV4_0.CTAny
        case (symbolsV3_5.CTBoolean, _) => symbolsV4_0.CTBoolean
        case (symbolsV3_5.CTFloat, _) => symbolsV4_0.CTFloat
        case (symbolsV3_5.CTGeometry, _) => symbolsV4_0.CTGeometry
        case (symbolsV3_5.CTGraphRef, _) => symbolsV4_0.CTGraphRef
        case (symbolsV3_5.CTInteger, _) => symbolsV4_0.CTInteger
        case (symbolsV3_5.ListType(_), children: Seq[AnyRef]) => symbolsV4_0.CTList(children.head.asInstanceOf[symbolsV4_0.CypherType])
        case (symbolsV3_5.CTMap, _) => symbolsV4_0.CTMap
        case (symbolsV3_5.CTNode, _) => symbolsV4_0.CTNode
        case (symbolsV3_5.CTNumber, _) => symbolsV4_0.CTNumber
        case (symbolsV3_5.CTPath, _) => symbolsV4_0.CTPath
        case (symbolsV3_5.CTPoint, _) => symbolsV4_0.CTPoint
        case (symbolsV3_5.CTRelationship, _) => symbolsV4_0.CTRelationship
        case (symbolsV3_5.CTString, _) => symbolsV4_0.CTString

        case (SemanticDirectionV3_5.BOTH, _) => expressionsV4_0.SemanticDirection.BOTH
        case (SemanticDirectionV3_5.INCOMING, _) => expressionsV4_0.SemanticDirection.INCOMING
        case (SemanticDirectionV3_5.OUTGOING, _) => expressionsV4_0.SemanticDirection.OUTGOING

        case (irV3_5.SimplePatternLength, _) => irV4_0.SimplePatternLength

        case (plansV3_5.IncludeTies, _) => plansv4_0.IncludeTies
        case (plansV3_5.DoNotIncludeTies, _) => plansv4_0.DoNotIncludeTies

        case (irV3_5.HasHeaders, _) => irV4_0.HasHeaders
        case (irV3_5.NoHeaders, _) => irV4_0.NoHeaders

        case (plansV3_5.ExpandAll, _) => plansv4_0.ExpandAll
        case (plansV3_5.ExpandInto, _) => plansv4_0.ExpandInto

        case (plansV3_5.IndexOrderNone, _) => plansv4_0.IndexOrderNone
        case (plansV3_5.IndexOrderAscending, _) => plansv4_0.IndexOrderAscending
        case (plansV3_5.IndexOrderDescending, _) => plansv4_0.IndexOrderDescending

        case (plansV3_5.DoNotGetValue, _) => plansv4_0.DoNotGetValue
        case (plansV3_5.CanGetValue, _) => plansv4_0.CanGetValue
        case (plansV3_5.GetValue, _) => plansv4_0.GetValue

        case (expressionsV3_5.NilPathStep, _) => expressionsV4_0.NilPathStep


        // ---------------- OTHER CLASSES ----------------
        // This comes after case objects, since we match also match on traits extended by case objects

        // Other classes in the logical plan package
        case (item@(_: plansV3_5.CypherValue |
                    _: plansV3_5.QualifiedName |
                    _: plansV3_5.FieldSignature |
                    _: plansV3_5.ProcedureAccessMode |
                    _: plansV3_5.QueryExpression[_] |
                    _: plansV3_5.SeekableArgs |
                    _: plansV3_5.ColumnOrder |
                    _: plansV3_5.IndexedProperty |
                    _: plansV3_5.ProcedureSignature |
                    _: plansV3_5.Bound[_] |
                    _: plansV3_5.SeekRange[_]), children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children)

        // Other classes in the IR package
        case (item@(_: irV3_5.PatternRelationship |
                    _: irV3_5.VarPatternLength |
                    _: irV3_5.CreateNode |
                    _: irV3_5.CreateRelationship), children: Seq[AnyRef]) =>
          convertVersion(oldIRPackage, newIRPackage, item, children)

        case (_: utilV3_5.ExhaustiveShortestPathForbiddenException, _) => new utilV4_0.ExhaustiveShortestPathForbiddenException

        case (spp: irV3_5.ShortestPathPattern, children: Seq[AnyRef]) =>
          val sp3_5 = convertASTNode[expressionsV4_0.ShortestPaths](spp.expr, expressionMap, planningAttributes3_5, planningAttributes4_0, ids, seenBySemanticTable)
          irV4_0.ShortestPathPattern(children(0).asInstanceOf[Option[String]], children(1).asInstanceOf[irV4_0.PatternRelationship], children(2).asInstanceOf[Boolean])(sp3_5)

        case (_: expressionsV3_5.SingleRelationshipPathStep, children: Seq[AnyRef]) =>
          expressionsV4_0.SingleRelationshipPathStep(children(0).asInstanceOf[ExpressionV4_0],
                                                     children(1).asInstanceOf[expressionsV4_0.SemanticDirection],
                                                     None, children(2).asInstanceOf[expressionsV4_0.PathStep])

        case (_: expressionsV3_5.MultiRelationshipPathStep, children: Seq[AnyRef]) =>
          expressionsV4_0.MultiRelationshipPathStep(children(0).asInstanceOf[ExpressionV4_0],
                                                     children(1).asInstanceOf[expressionsV4_0.SemanticDirection],
                                                     None, children(2).asInstanceOf[expressionsV4_0.PathStep])

        case (item@(_: expressionsV3_5.PathStep | _: expressionsV3_5.NameToken[_]), children: Seq[AnyRef]) =>
          convertVersion(oldExpressionPackage, newExpressionPackage, item, children)

        case (utilV3_5.Fby(head, tail), children: Seq[AnyRef]) => utilV4_0.Fby(children(0), children(1).asInstanceOf[utilV4_0.NonEmptyList[_]])

        case (utilV3_5.Last(head), children: Seq[AnyRef]) => utilV4_0.Last(children(0))

        case (nameId: utilV3_5.NameId, children: Seq[AnyRef]) =>
          convertVersion(oldUtilPackage, newUtilPackage, nameId, children)

      }.apply(before)

      before._1 match {
        case plan: LogicalPlanV3_5 =>
          val plan4_0 = rewritten.asInstanceOf[LogicalPlanV4_0]
          // Set attributes
          if (planningAttributes3_5.solveds.isDefinedAt(plan.id)) {
            planningAttributes4_0.solveds.set(plan4_0.id, new PlannerQueryWrapper(planningAttributes3_5.solveds.get(plan.id)))
          }
          if (planningAttributes3_5.cardinalities.isDefinedAt(plan.id)) {
            planningAttributes4_0.cardinalities.set(plan4_0.id, helpers.as4_0(planningAttributes3_5.cardinalities.get(plan.id)))
          }
          if (planningAttributes3_5.providedOrders.isDefinedAt(plan.id)) {
            planningAttributes4_0.providedOrders.set(plan4_0.id, helpers.as4_0(planningAttributes3_5.providedOrders.get(plan.id)))
          }
        // Save Mapping from 3.5 expression to 4.0 expression
        case e: ExpressionV3_5 if seenBySemanticTable(e) => expressionMap += (((e, e.position), rewritten.asInstanceOf[ExpressionV4_0]))
        case _ =>
      }
      rewritten
    }
  }

  /**
    * Converts a logical plan. It will keep the same id and will also set solved and cardinality on the converted plan
    * Returns also a mapping from old to new expressions for all expressions that are imporant according to the
    * provided lambda.
    */
  def convertLogicalPlan[T <: LogicalPlanV4_0](logicalPlan: LogicalPlanV3_5,
                                               planningAttributes3_5: PlanningAttributesV3_5,
                                               planningAttributes4_0: PlanningAttributesV4_0,
                                               idConverter: IdConverter,
                                               seenBySemanticTable: ExpressionV3_5 => Boolean = _ => true): (T, ExpressionMapping4To5) = {
    val rewriter = new LogicalPlanRewriter(planningAttributes3_5, planningAttributes4_0, idConverter, seenBySemanticTable = seenBySemanticTable)
    val planv4_0 = new RewritableAny[LogicalPlanV3_5](logicalPlan).rewrite(rewriter, Seq.empty).asInstanceOf[T]
    (planv4_0, rewriter.expressionMap.toMap)
  }

  /**
    * Converts an expression.
    */
  private[v3_5] def convertExpression[T <: ExpressionV4_0](expression: ExpressionV3_5,
                                                           planningAttributes3_5: PlanningAttributesV3_5,
                                                           planningAttributes4_0: PlanningAttributesV4_0,
                                                           idConverter: IdConverter): T = {
    new RewritableAny[ExpressionV3_5](expression)
      .rewrite(new LogicalPlanRewriter(planningAttributes3_5, planningAttributes4_0, idConverter), Seq.empty)
      .asInstanceOf[T]
  }

  /**
    * Converts an AST node.
    */
  private def convertASTNode[T <: utilV4_0.ASTNode](ast: utilV3_5.ASTNode,
                                                    expressionMap: MutableExpressionMapping3To4,
                                                    planningAttributes3_5: PlanningAttributesV3_5,
                                                    planningAttributes4_0: PlanningAttributesV4_0,
                                                    idConverter: IdConverter,
                                                    seenBySemanticTable: ExpressionV3_5 => Boolean): T = {
    new RewritableAny[utilV3_5.ASTNode](ast)
      .rewrite(new LogicalPlanRewriter(planningAttributes3_5,
                                       planningAttributes4_0,
                                       idConverter,
                                       expressionMap,
                                       seenBySemanticTable), Seq.empty)
      .asInstanceOf[T]
  }

  /**
    * A cache for all constructors we encounter so we don't have to use reflection all over again
    * to find the constructors every time.
    */
  private val constructors = new ThreadLocal[MutableHashMap[(String, String, String), Constructor[_]]]() {
    override def initialValue: MutableHashMap[(String, String, String), Constructor[_]] =
      new MutableHashMap[(String, String, String), Constructor[_]]
  }

  /**
    * Given the class name in 3.5 and the old and new package names, return the constructor of the
    * 4.0 class with the same name.
    */
  private def getConstructor(classNameV3_5: String, oldPackage: String, newPackage: String): Constructor[_] = {
    constructors.get.getOrElseUpdate((classNameV3_5, oldPackage, newPackage), {
      assert(classNameV3_5.contains(oldPackage), s"wrong 3.5 package name given. $classNameV3_5 does not contain $oldPackage")
      val classNamev4_0 = classNameV3_5.replace(oldPackage, newPackage)
      Try(Class.forName(classNamev4_0)).map(_.getConstructors.head) match {
        case Success(c) => c
        case Failure(e: ClassNotFoundException) => throw new InternalException(
          s"Failed trying to rewrite $classNameV3_5 - 4.0 class not found ($classNamev4_0)", e)
        case Failure(e: NoSuchElementException) => throw new InternalException(
          s"Failed trying to rewrite $classNameV3_5 - this class does not have a constructor", e)
        case Failure(e) => throw e
      }
    })
  }

  /**
    * Convert something (expression, AstNode, LogicalPlan) from 3.5 to 4.0.
    *
    * @param oldPackage the package name in 3.5
    * @param newPackage the package name in 4.0
    * @param thing      the thing to convert
    * @param children   the already converted children, which will be used as constructor arguments when constructing the
    *                   converted thing
    * @param extraArg
    *                   if there is second set of constructor arguments, this will be the first argument in the second set.
    *                   Otherwise `null`.
    * @param assignableClazzForArg
    *                              The class of `extraArg`. Used to check if the constructor matches type with that argument.
    * @param extraArg2
    *                   if there is second set of constructor arguments, this will be the second argument in the second set.
    *                   Otherwise `null`.
    * @param assignableClazzForArg2
    *                              The class of `extraArg2`. Used to check if the constructor matches type with that argument.
    * @return the converted thing.
    */
  private def convertVersion(oldPackage: String,
                             newPackage: String,
                             thing: AnyRef,
                             children: Seq[AnyRef],
                             extraArg: AnyRef = null,
                             assignableClazzForArg: Class[_] = null,
                             extraArg2: AnyRef = null,
                             assignableClazzForArg2: Class[_] = null): AnyRef = {
    val thingClass = thing.getClass
    val classNameV3_5 = thingClass.getName
    val constructor = getConstructor(classNameV3_5, oldPackage, newPackage)

    val params = constructor.getParameterTypes
    val args = children.toVector
    val ctorArgs =
      if (params.length == args.length + 1
        && params.last.isAssignableFrom(assignableClazzForArg))
        args :+ extraArg
      else if (params.length == args.length + 2
        && params(params.length - 2).isAssignableFrom(assignableClazzForArg)
        && params(params.length - 1).isAssignableFrom(assignableClazzForArg2))
        args :+ extraArg :+ extraArg2
      else
        args

    try {
      constructor.newInstance(ctorArgs: _*).asInstanceOf[AnyRef]
    }
    catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Could not construct ${thingClass.getSimpleName} with arguments ${ctorArgs.toList}", e)
    }
  }
}
