/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4

import java.lang.reflect.Constructor

import org.neo4j.cypher.internal.compatibility.v3_4.SemanticTableConverter.ExpressionMapping4To5
import org.neo4j.cypher.internal.compiler.v3_5.helpers.PredicateHelper
import org.neo4j.cypher.internal.frontend.{v3_4 => frontendV3_4}
import org.neo4j.cypher.internal.ir.v3_5.{CreateNode, CreateRelationship}
import org.neo4j.cypher.internal.ir.{v3_4 => irV3_4, v3_5 => irv3_5}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_4, Solveds => SolvedsV3_4}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_5, Solveds => SolvedsV3_5}
import org.neo4j.cypher.internal.util.v3_4.{InputPosition => InputPositionV3_4, symbols => symbolsV3_4}
import org.neo4j.cypher.internal.util.{v3_4 => utilv3_4}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4, SemanticDirection => SemanticDirectionV3_4}
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan => LogicalPlanV3_4}
import org.neo4j.cypher.internal.v3_4.logical.{plans => plansV3_4}
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsv3_4}
import org.neo4j.cypher.internal.v3_5.expressions.{LogicalVariable, PropertyKeyName, Expression => Expressionv3_5, LabelName => LabelNamev3_5, RelTypeName => RelTypeNamev3_5, SemanticDirection => SemanticDirectionv3_5}
import org.neo4j.cypher.internal.v3_5.logical.plans.{DoNotGetValue, FieldSignature, IndexOrderNone, IndexedProperty, ProcedureAccessMode, QualifiedName, UserFunctionSignature, LogicalPlan => LogicalPlanv3_5}
import org.neo4j.cypher.internal.v3_5.logical.{plans => plansv3_5}
import org.neo4j.cypher.internal.v3_5.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen
import org.neo4j.cypher.internal.v3_5.util.symbols.CypherType
import org.neo4j.cypher.internal.v3_5.util.{symbols => symbolsv3_5, _}
import org.neo4j.cypher.internal.v3_5.{expressions => expressionsv3_5, util => utilv3_5}

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.{Failure, Success, Try}

/**
  * This is responsible for converting logical plans from the old version to the current version.
  */
object LogicalPlanConverter {

  type MutableExpressionMapping3To4 = mutable.Map[(ExpressionV3_4, InputPositionV3_4), Expressionv3_5]

  val oldLogicalPlanPackage = "org.neo4j.cypher.internal.v3_4.logical.plans"
  val newLogicalPlanPackage = "org.neo4j.cypher.internal.v3_5.logical.plans"
  val oldASTPackage = "org.neo4j.cypher.internal.frontend.v3_4.ast"
  val newASTPackage = "org.neo4j.cypher.internal.v3_5.ast"
  val oldExpressionPackage = "org.neo4j.cypher.internal.v3_4.expressions"
  val newExpressionPackage = "org.neo4j.cypher.internal.v3_5.expressions"
  val oldUtilPackage = "org.neo4j.cypher.internal.util.v3_4"
  val newUtilPackage = "org.neo4j.cypher.internal.v3_5.util"
  val oldIRPackage = "org.neo4j.cypher.internal.ir.v3_4"
  val newIRPackage = "org.neo4j.cypher.internal.ir.v3_5"
  val oldRewritersPackage = "org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters"

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
  private class LogicalPlanRewriter(solveds3_4: SolvedsV3_4,
                                    cardinalities3_4: CardinalitiesV3_4,
                                    solveds3_5: SolvedsV3_5,
                                    cardinalities3_5: CardinalitiesV3_5,
                                    ids: IdConverter,
                                    val expressionMap: MutableExpressionMapping3To4 = new mutable.HashMap[(ExpressionV3_4, InputPositionV3_4), Expressionv3_5],
                                    val seenBySemanticTable: ExpressionV3_4 => Boolean = _ => true)
    extends RewriterWithArgs {

    override def apply(v1: (AnyRef, Seq[AnyRef])): AnyRef = rewriter.apply(v1)

    private val rewriter: RewriterWithArgs = bottomUpWithArgs { before =>
      val rewritten = RewriterWithArgs.lift {

        case ( plan:plansV3_4.Selection, children: Seq[AnyRef]) =>
          plansv3_5.Selection(PredicateHelper.coercePredicates(children(0).asInstanceOf[Seq[Expressionv3_5]]),
                              children(1).asInstanceOf[LogicalPlanv3_5]
                              )(ids.convertId(plan))

        case (plan: plansV3_4.CreateNode, children: Seq[AnyRef]) =>
          flattenCreates(
            children(0).asInstanceOf[LogicalPlanv3_5],
            Some(CreateNode(
              plan.idName,
              children(2).asInstanceOf[Seq[LabelNamev3_5]],
              children(3).asInstanceOf[Option[Expressionv3_5]]
            )),
            None,
            ids.convertId(plan)
          )

        case (plan: plansV3_4.CreateRelationship, children: Seq[AnyRef]) =>
          flattenCreates(
            children(0).asInstanceOf[LogicalPlanv3_5],
            None,
            Some(CreateRelationship(
              plan.idName,
              plan.startNode,
              children(3).asInstanceOf[RelTypeNamev3_5],
              plan.endNode,
              SemanticDirectionv3_5.OUTGOING, // as we always provide the start node as "left" and
                                              // the end node and "right", the direction is always OUTGOING
              children(5).asInstanceOf[Option[Expressionv3_5]]
            )),
            ids.convertId(plan)
          )

        case (item: expressionsv3_4.RelationshipPattern, children: Seq[AnyRef]) =>
          expressionsv3_5.RelationshipPattern(
            children(0).asInstanceOf[Option[expressionsv3_5.LogicalVariable]],
            children(1).asInstanceOf[Seq[expressionsv3_5.RelTypeName]],
            children(2).asInstanceOf[Option[Option[expressionsv3_5.Range]]],
            children(3).asInstanceOf[Option[expressionsv3_5.Expression]],
            children(4).asInstanceOf[expressionsv3_5.SemanticDirection],
            children(5).asInstanceOf[Boolean],
            None
          )(helpers.as3_5(item.position))

        case (item: expressionsv3_4.NodePattern, children: Seq[AnyRef]) =>
          expressionsv3_5.NodePattern(
            children(0).asInstanceOf[Option[expressionsv3_5.LogicalVariable]],
            children(1).asInstanceOf[Seq[expressionsv3_5.LabelName]],
            children(2).asInstanceOf[Option[expressionsv3_5.Expression]],
            None
          )(helpers.as3_5(item.position))

        case (plan: plansV3_4.SetRelationshipPropery, children: Seq[AnyRef]) =>
          plansv3_5.SetRelationshipProperty(
            children(0).asInstanceOf[LogicalPlanv3_5],
            children(1).asInstanceOf[String],
            children(2).asInstanceOf[PropertyKeyName],
            children(3).asInstanceOf[Expressionv3_5]
          )(ids.convertId(plan))

        case (plan: plansV3_4.NodeIndexContainsScan, children: Seq[AnyRef]) =>
          plansv3_5.NodeIndexContainsScan(
            children(0).asInstanceOf[String],
            children(1).asInstanceOf[expressionsv3_5.LabelToken],
            IndexedProperty(children(2).asInstanceOf[expressionsv3_5.PropertyKeyToken], DoNotGetValue),
            children(3).asInstanceOf[Expressionv3_5],
            children(4).asInstanceOf[Set[String]],
            IndexOrderNone
          )(ids.convertId(plan))

        case (plan: plansV3_4.NodeIndexEndsWithScan, children: Seq[AnyRef]) =>
          plansv3_5.NodeIndexEndsWithScan(
            children(0).asInstanceOf[String],
            children(1).asInstanceOf[expressionsv3_5.LabelToken],
            IndexedProperty(children(2).asInstanceOf[expressionsv3_5.PropertyKeyToken], DoNotGetValue),
            children(3).asInstanceOf[Expressionv3_5],
            children(4).asInstanceOf[Set[String]],
            IndexOrderNone
          )(ids.convertId(plan))

        case (plan: plansV3_4.NodeIndexScan, children: Seq[AnyRef]) =>
          plansv3_5.NodeIndexScan(
            children(0).asInstanceOf[String],
            children(1).asInstanceOf[expressionsv3_5.LabelToken],
            IndexedProperty(children(2).asInstanceOf[expressionsv3_5.PropertyKeyToken], DoNotGetValue),
            children(3).asInstanceOf[Set[String]],
            IndexOrderNone
          )(ids.convertId(plan))

        case (plan: plansV3_4.NodeIndexSeek, children: Seq[AnyRef]) =>
          plansv3_5.NodeIndexSeek(
            children(0).asInstanceOf[String],
            children(1).asInstanceOf[expressionsv3_5.LabelToken],
            children(2).asInstanceOf[Seq[expressionsv3_5.PropertyKeyToken]].map(IndexedProperty(_, DoNotGetValue)),
            children(3).asInstanceOf[plansv3_5.QueryExpression[expressionsv3_5.Expression]],
            children(4).asInstanceOf[Set[String]],
            IndexOrderNone
          )(ids.convertId(plan))

        case (plan: plansV3_4.NodeUniqueIndexSeek, children: Seq[AnyRef]) =>
          plansv3_5.NodeUniqueIndexSeek(
            children(0).asInstanceOf[String],
            children(1).asInstanceOf[expressionsv3_5.LabelToken],
            children(2).asInstanceOf[Seq[expressionsv3_5.PropertyKeyToken]].map(IndexedProperty(_, DoNotGetValue)),
            children(3).asInstanceOf[plansv3_5.QueryExpression[expressionsv3_5.Expression]],
            children(4).asInstanceOf[Set[String]],
            IndexOrderNone
          )(ids.convertId(plan))

          // Fallthrough for all plans
        case (plan: plansV3_4.LogicalPlan, children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, plan, children, ids.convertId(plan), classOf[IdGen])

        case (item@(_: plansV3_4.PrefixSeekRangeWrapper |
                    _: plansV3_4.InequalitySeekRangeWrapper |
                    _: plansV3_4.PointDistanceSeekRangeWrapper |
                    _: plansV3_4.NestedPlanExpression |
                    _: plansV3_4.ResolvedFunctionInvocation), children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children, helpers.as3_5(item.asInstanceOf[utilv3_4.ASTNode].position), classOf[InputPosition])

          // TODO this seems unnecessary
        case (item: frontendV3_4.ast.rewriters.DesugaredMapProjection, children: Seq[AnyRef]) =>
          convertVersion(oldRewritersPackage, newExpressionPackage, item, children, helpers.as3_5(item.position), classOf[InputPosition])

        case (item: frontendV3_4.ast.ProcedureResultItem, children: Seq[AnyRef]) =>
          convertVersion(oldASTPackage, newASTPackage, item, children, helpers.as3_5(item.position), classOf[InputPosition])

        case (funcV3_4@expressionsv3_4.FunctionInvocation(_, expressionsv3_4.FunctionName("timestamp"), _, _), _: Seq[AnyRef]) => {
          val datetimeSignature = UserFunctionSignature(
            QualifiedName(Seq(), "datetime"),
            inputSignature = IndexedSeq.empty,
            symbolsv3_5.CTDateTime,
            deprecationInfo = None,
            allowed = Array.empty,
            description = None,
            isAggregate = false,
            id = None // will use by-name lookup for built-in functions for that came from a 3.4 plan, since timestamp is not a user-defined function in 3.4
          )
          val funcPosV3_5 = helpers.as3_5(funcV3_4.functionName.position)
          val datetimeFuncV3_5 = plansv3_5.ResolvedFunctionInvocation(
            QualifiedName(Seq(), "datetime"),
            Some(datetimeSignature),
            callArguments = IndexedSeq.empty
          )(funcPosV3_5)

          val epochMillisV3_5 = expressionsv3_5.Property(
            datetimeFuncV3_5,
            expressionsv3_5.PropertyKeyName("epochMillis")(funcPosV3_5)
          )(funcPosV3_5)

          epochMillisV3_5
        }

        case (item: expressionsv3_4.PatternComprehension, children: Seq[AnyRef]) =>
          expressionsv3_5.PatternComprehension(
            children(0).asInstanceOf[Option[LogicalVariable]],
            children(1).asInstanceOf[expressionsv3_5.RelationshipsPattern],
            children(2).asInstanceOf[Option[expressionsv3_5.Expression]],
            children(3).asInstanceOf[expressionsv3_5.Expression]
          )(helpers.as3_5(item.position), item.outerScope.map(v => expressionsv3_5.Variable(v.name)(helpers.as3_5(v.position))))

        case (item: expressionsv3_4.MapProjection, children: Seq[AnyRef]) =>
          expressionsv3_5.MapProjection(
            children(0).asInstanceOf[expressionsv3_5.Variable],
            children(1).asInstanceOf[Seq[expressionsv3_5.MapProjectionElement]]
          )(helpers.as3_5(item.position), item.definitionPos.map(helpers.as3_5))

        case (item: plansV3_4.ResolvedCall, children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children, helpers.as3_5(item.position), classOf[InputPosition])

        case (funcV3_4:expressionsv3_4.FunctionInvocation, children: Seq[AnyRef]) =>
          convertVersion(oldExpressionPackage, newExpressionPackage, funcV3_4, children:+Boolean.box(false), helpers.as3_5(funcV3_4.position), classOf[InputPosition])

        case (listCompV3_4:expressionsv3_4.ListComprehension, children: Seq[AnyRef]) =>
          convertVersion(oldExpressionPackage, newExpressionPackage, listCompV3_4, children:+Boolean.box(false), helpers.as3_5(listCompV3_4.position), classOf[InputPosition])

        // Fallthrough for all ASTNodes
        case (expressionV3_4: utilv3_4.ASTNode, children: Seq[AnyRef]) =>
          convertVersion(oldExpressionPackage, newExpressionPackage, expressionV3_4, children, helpers.as3_5(expressionV3_4.position), classOf[InputPosition])

        case (symbolsV3_4.CTAny, _) => symbolsv3_5.CTAny
        case (symbolsV3_4.CTBoolean, _) => symbolsv3_5.CTBoolean
        case (symbolsV3_4.CTFloat, _) => symbolsv3_5.CTFloat
        case (symbolsV3_4.CTGeometry, _) => symbolsv3_5.CTGeometry
        case (symbolsV3_4.CTGraphRef, _) => symbolsv3_5.CTGraphRef
        case (symbolsV3_4.CTInteger, _) => symbolsv3_5.CTInteger
        case (symbolsV3_4.ListType(_), children: Seq[AnyRef]) => symbolsv3_5.CTList(children.head.asInstanceOf[symbolsv3_5.CypherType])
        case (symbolsV3_4.CTMap, _) => symbolsv3_5.CTMap
        case (symbolsV3_4.CTNode, _) => symbolsv3_5.CTNode
        case (symbolsV3_4.CTNumber, _) => symbolsv3_5.CTNumber
        case (symbolsV3_4.CTPath, _) => symbolsv3_5.CTPath
        case (symbolsV3_4.CTPoint, _) => symbolsv3_5.CTPoint
        case (symbolsV3_4.CTRelationship, _) => symbolsv3_5.CTRelationship
        case (symbolsV3_4.CTString, _) => symbolsv3_5.CTString

        case (SemanticDirectionV3_4.BOTH, _) => expressionsv3_5.SemanticDirection.BOTH
        case (SemanticDirectionV3_4.INCOMING, _) => expressionsv3_5.SemanticDirection.INCOMING
        case (SemanticDirectionV3_4.OUTGOING, _) => expressionsv3_5.SemanticDirection.OUTGOING

        case (irV3_4.SimplePatternLength, _) => irv3_5.SimplePatternLength

        case (plansV3_4.IncludeTies, _) => plansv3_5.IncludeTies
        case (plansV3_4.DoNotIncludeTies, _) => plansv3_5.DoNotIncludeTies

        case (irV3_4.HasHeaders, _) => irv3_5.HasHeaders
        case (irV3_4.NoHeaders, _) => irv3_5.NoHeaders

        case (plansV3_4.ExpandAll, _) => plansv3_5.ExpandAll
        case (plansV3_4.ExpandInto, _) => plansv3_5.ExpandInto

        case (_: utilv3_4.ExhaustiveShortestPathForbiddenException, _) => new utilv3_5.ExhaustiveShortestPathForbiddenException

        case (spp: irV3_4.ShortestPathPattern, children: Seq[AnyRef]) =>
          val sp3_4 = convertASTNode[expressionsv3_5.ShortestPaths](spp.expr, expressionMap, solveds3_4, cardinalities3_4, solveds3_5, cardinalities3_5, ids, seenBySemanticTable)
          irv3_5.ShortestPathPattern(children(0).asInstanceOf[Option[String]], children(1).asInstanceOf[irv3_5.PatternRelationship], children(2).asInstanceOf[Boolean])(sp3_4)

        case (expressionsv3_4.NilPathStep, _) => expressionsv3_5.NilPathStep

        case (item@(_: expressionsv3_4.PathStep | _: expressionsv3_4.NameToken[_]), children: Seq[AnyRef]) =>
          convertVersion(oldExpressionPackage, newExpressionPackage, item, children)

        case (nameId: utilv3_4.NameId, children: Seq[AnyRef]) =>
          convertVersion(oldUtilPackage, newUtilPackage, nameId, children)

        case (utilv3_4.Fby(head, tail), children: Seq[AnyRef]) => utilv3_5.Fby(children(0), children(1).asInstanceOf[utilv3_5.NonEmptyList[_]])

        case (utilv3_4.Last(head), children: Seq[AnyRef]) => utilv3_5.Last(children(0))

        case ( _:plansV3_4.ProcedureSignature, children: Seq[AnyRef]) =>
         plansv3_5.ProcedureSignature(children(0).asInstanceOf[QualifiedName],
                                      children(1).asInstanceOf[IndexedSeq[FieldSignature]],
                                      children(2).asInstanceOf[Option[IndexedSeq[FieldSignature]]],
                                      children(3).asInstanceOf[Option[String]],
                                      children(4).asInstanceOf[ProcedureAccessMode],
                                      children(5).asInstanceOf[Option[String]],
                                      children(6).asInstanceOf[Option[String]],
                                      children(7).asInstanceOf[Boolean],
                                      children(8).asInstanceOf[Option[Int]])

        case ( _:plansV3_4.UserFunctionSignature, children: Seq[AnyRef]) =>
          plansv3_5.UserFunctionSignature(children(0).asInstanceOf[QualifiedName],
                                       children(1).asInstanceOf[IndexedSeq[FieldSignature]],
                                       children(2).asInstanceOf[CypherType],
                                       children(3).asInstanceOf[Option[String]],
                                       children(4).asInstanceOf[Array[String]],
                                       children(5).asInstanceOf[Option[String]],
                                       children(6).asInstanceOf[Boolean],
                                       children(7).asInstanceOf[Option[Int]])

        case (item@(_: plansV3_4.CypherValue |
                    _: plansV3_4.QualifiedName |
                    _: plansV3_4.FieldSignature |
                    _: plansV3_4.ProcedureAccessMode |
                    _: plansV3_4.QueryExpression[_] |
                    _: plansV3_4.SeekableArgs |
                    _: plansV3_4.ColumnOrder), children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children)

        case (item@(_: irV3_4.PatternRelationship |
                    _: irV3_4.VarPatternLength), children: Seq[AnyRef]) =>
          convertVersion(oldIRPackage, newIRPackage, item, children)

        case (item: plansV3_4.Bound[_], children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children)

        case (item: plansV3_4.SeekRange[_], children: Seq[AnyRef]) =>
          convertVersion(oldLogicalPlanPackage, newLogicalPlanPackage, item, children)
      }.apply(before)

      before._1 match {
        case plan: LogicalPlanV3_4 =>
          val plan3_5 = rewritten.asInstanceOf[LogicalPlanv3_5]
          // Set attributes
          if (solveds3_4.isDefinedAt(plan.id)) {
            solveds3_5.set(plan3_5.id, new PlannerQueryWrapper(solveds3_4.get(plan.id)))
          }
          if (cardinalities3_4.isDefinedAt(plan.id)) {
            cardinalities3_5.set(plan3_5.id, helpers.as3_5(cardinalities3_4.get(plan.id)))
          }
        // Save Mapping from 3.4 expression to 3.5 expression
        case e: ExpressionV3_4 if seenBySemanticTable(e) => expressionMap += (((e, e.position), rewritten.asInstanceOf[Expressionv3_5]))
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
  def convertLogicalPlan[T <: LogicalPlanv3_5](logicalPlan: LogicalPlanV3_4,
                                               solveds3_4: SolvedsV3_4,
                                               cardinalities3_4: CardinalitiesV3_4,
                                               solveds3_5: SolvedsV3_5,
                                               cardinalities3_5: CardinalitiesV3_5,
                                               idConverter: IdConverter,
                                               seenBySemanticTable: ExpressionV3_4 => Boolean = _ => true): (LogicalPlanv3_5, ExpressionMapping4To5) = {
    val rewriter = new LogicalPlanRewriter(solveds3_4, cardinalities3_4, solveds3_5, cardinalities3_5, idConverter, seenBySemanticTable = seenBySemanticTable)
    val planv3_5 = new RewritableAny[LogicalPlanV3_4](logicalPlan).rewrite(rewriter, Seq.empty).asInstanceOf[T]
    (planv3_5, rewriter.expressionMap.toMap)
  }

  /**
    * Converts an expression.
    */
  private[v3_4] def convertExpression[T <: Expressionv3_5](expression: ExpressionV3_4,
                                                           solveds3_4: SolvedsV3_4,
                                                           cardinalities3_4: CardinalitiesV3_4,
                                                           solveds3_5: SolvedsV3_5,
                                                           cardinalities3_5: CardinalitiesV3_5,
                                                           idConverter: IdConverter): T = {
    new RewritableAny[ExpressionV3_4](expression)
      .rewrite(new LogicalPlanRewriter(solveds3_4, cardinalities3_4, solveds3_5, cardinalities3_5, idConverter), Seq.empty)
      .asInstanceOf[T]
  }

  /**
    * Converts an AST node.
    */
  private def convertASTNode[T <: utilv3_5.ASTNode](ast: utilv3_4.ASTNode,
                                                    expressionMap: MutableExpressionMapping3To4,
                                                    solveds3_4: SolvedsV3_4,
                                                    cardinalities3_4: CardinalitiesV3_4,
                                                    solveds3_5: SolvedsV3_5,
                                                    cardinalities3_5: CardinalitiesV3_5,
                                                    idConverter: IdConverter,
                                                    seenBySemanticTable: ExpressionV3_4 => Boolean): T = {
    new RewritableAny[utilv3_4.ASTNode](ast)
      .rewrite(new LogicalPlanRewriter(solveds3_4,
                                       cardinalities3_4,
                                       solveds3_5,
                                       cardinalities3_5,
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
    * Given the class name in 3.4 and the old and new package names, return the constructor of the
    * 3.5 class with the same name.
    */
  private def getConstructor(classNameV3_4: String, oldPackage: String, newPackage: String): Constructor[_] = {
    constructors.get.getOrElseUpdate((classNameV3_4, oldPackage, newPackage), {
      assert(classNameV3_4.contains(oldPackage), s"wrong 3.4 package name given. $classNameV3_4 does not contain $oldPackage")
      val classNamev3_5 = classNameV3_4.replace(oldPackage, newPackage)
      Try(Class.forName(classNamev3_5)).map(_.getConstructors.head) match {
        case Success(c) => c
        case Failure(e: ClassNotFoundException) => throw new InternalException(
          s"Failed trying to rewrite $classNameV3_4 - 3.5 class not found ($classNamev3_5)", e)
        case Failure(e: NoSuchElementException) => throw new InternalException(
          s"Failed trying to rewrite $classNameV3_4 - this class does not have a constructor", e)
        case Failure(e) => throw e
      }
    })
  }

  /**
    * Convert something (expression, AstNode, LogicalPlan) from 3.4 to 3.5.
    *
    * @param oldPackage the package name in 3.4
    * @param newPackage the package name in 3.5
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
    val classNameV3_4 = thingClass.getName
    val constructor = getConstructor(classNameV3_4, oldPackage, newPackage)

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

    Try(constructor.newInstance(ctorArgs: _*).asInstanceOf[AnyRef]) match {
      case Success(i) => i
      case Failure(e) =>
        throw new IllegalArgumentException(s"Could not construct ${thingClass.getSimpleName} with arguments ${ctorArgs.toList}", e)
    }
  }

  /**
    * Converts a 3.4 CreateNode or CreateRelationship logical plan operator into a 3.5 Create operator.
    *
    * If the source operator is a Create operator, the create command is added to that operator instead of creating a
    * new one, effectively squashing deep tree of 3.3 CreateNode and CreateRelationship operators into on 3.5 Create.
    */
  private def flattenCreates(source: LogicalPlanv3_5,
                             createNode: Option[CreateNode],
                             createRelationship: Option[CreateRelationship],
                             id: IdGen): plansv3_5.Create = {
    source match {
      case plansv3_5.Create(source, nodes, relationships) =>
        plansv3_5.Create(source, nodes ++ createNode, relationships ++ createRelationship)(id)

      case nonCreate =>
        plansv3_5.Create(nonCreate, createNode.toSeq, createRelationship.toSeq)(id)
    }
  }
}
