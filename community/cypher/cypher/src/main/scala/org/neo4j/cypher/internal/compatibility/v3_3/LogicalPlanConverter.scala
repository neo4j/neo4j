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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.lang.reflect.Constructor

import org.neo4j.cypher.internal.compatibility.v3_3.SemanticTableConverter.ExpressionMapping3To4
import org.neo4j.cypher.internal.compiler.{v3_3 => compilerV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition => InputPositionV3_3, SemanticDirection => SemanticDirectionV3_3, ast => astV3_3, symbols => symbolsV3_3}
import org.neo4j.cypher.internal.frontend.{v3_3 => frontendV3_3}
import org.neo4j.cypher.internal.ir.v3_5.{CSVFormat, CreateNode, CreateRelationship}
import org.neo4j.cypher.internal.ir.{v3_3 => irV3_3, v3_5 => irv3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.opencypher.v9_0.util.Rewritable.RewritableAny
import org.opencypher.v9_0.util.attribution.{IdGen, SequentialIdGen}
import org.opencypher.v9_0.util.symbols.CypherType
import org.opencypher.v9_0.util.{symbols => symbolsv3_5, _}
import org.opencypher.v9_0.{util => utilv3_5}
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan => LogicalPlanV3_3}
import org.neo4j.cypher.internal.v3_3.logical.{plans => plansV3_3}
import org.opencypher.v9_0.expressions.{Expression => Expressionv3_5, LabelName => LabelNamev3_5, RelTypeName => RelTypeNamev3_5, SemanticDirection => SemanticDirectionv3_5}
import org.neo4j.cypher.internal.v3_5.logical.plans.{FieldSignature, ProcedureAccessMode, QualifiedName, LogicalPlan => LogicalPlanv3_5}
import org.neo4j.cypher.internal.v3_5.logical.{plans => plansv3_5}
import org.opencypher.v9_0.{expressions => expressionsv3_5}

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.{Failure, Success, Try}

object LogicalPlanConverter {

  type MutableExpressionMapping3To4 = mutable.Map[(ExpressionV3_3, InputPositionV3_3), Expressionv3_5]

  private val v3_3_AST = "org.neo4j.cypher.internal.frontend.v3_3.ast"

  //noinspection ZeroIndexToHead
  private class LogicalPlanRewriter(solveds: Solveds,
                                    cardinalities: Cardinalities,
                                    ids: IdConverter,
                                    val expressionMap: MutableExpressionMapping3To4 = new mutable.HashMap[(ExpressionV3_3, InputPositionV3_3), Expressionv3_5],
                                    val isImportant: ExpressionV3_3 => Boolean = _ => true)
    extends RewriterWithArgs {

    val procedureOrSchemaIdGen = new SequentialIdGen()

    override def apply(v1: (AnyRef, Seq[AnyRef])): AnyRef = rewriter.apply(v1)

    private val rewriter: RewriterWithArgs = bottomUpWithArgs { before =>
      val rewritten = RewriterWithArgs.lift {
        case ( plan:plansV3_3.LoadCSV, children: Seq[AnyRef]) =>
          plansv3_5.LoadCSV(children(0).asInstanceOf[LogicalPlanv3_5],
                            children(1).asInstanceOf[Expressionv3_5],
                            children(2).asInstanceOf[String],
                            children(3).asInstanceOf[CSVFormat],
                            children(4).asInstanceOf[Option[String]],
                            children(5).asInstanceOf[Boolean],
                            CSVResources.DEFAULT_BUFFER_SIZE)(ids.convertId(plan))

        case (plan: plansV3_3.Argument, children: Seq[AnyRef]) =>
          plansv3_5.Argument(children.head.asInstanceOf[Set[String]])(ids.convertId(plan))

        case (plan: plansV3_3.SingleRow, _) =>
          plansv3_5.Argument()(ids.convertId(plan))

        case (plan: plansV3_3.CreateNode, children: Seq[AnyRef]) =>
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

        case (plan: plansV3_3.CreateRelationship, children: Seq[AnyRef]) =>
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

        case (plan: plansV3_3.ProduceResult, children: Seq[AnyRef]) =>
          plansv3_5.ProduceResult(source = children(1).asInstanceOf[LogicalPlanv3_5],
            columns = children(0).asInstanceOf[Seq[String]])(ids.convertId(plan))

        case (plan: plansV3_3.TriadicSelection, children: Seq[AnyRef]) =>
          plansv3_5.TriadicSelection(left = children(1).asInstanceOf[LogicalPlanv3_5],
            right = children(5).asInstanceOf[LogicalPlanv3_5],
            positivePredicate = children(0).asInstanceOf[Boolean],
            sourceId = children(2).asInstanceOf[String],
            seenId = children(3).asInstanceOf[String],
            targetId = children(4).asInstanceOf[String])(ids.convertId(plan))

        case (plan: plansV3_3.ProceduralLogicalPlan, children: Seq[AnyRef]) =>
          convertVersion("v3_3", "v3_5", plan, children, procedureOrSchemaIdGen, classOf[IdGen])

        case (plan: plansV3_3.OuterHashJoin, children: Seq[AnyRef]) =>
          plansv3_5.LeftOuterHashJoin(
            children(0).asInstanceOf[Set[String]],
            children(1).asInstanceOf[LogicalPlanv3_5],
            children(2).asInstanceOf[LogicalPlanv3_5]
          )(ids.convertId(plan))

        case (plan: plansV3_3.LogicalPlan, children: Seq[AnyRef]) =>
          convertVersion("v3_3", "v3_5", plan, children, ids.convertId(plan), classOf[IdGen])

        case (inp: astV3_3.InvalidNodePattern, children: Seq[AnyRef]) =>
          new expressionsv3_5.InvalidNodePattern(children.head.asInstanceOf[Option[expressionsv3_5.Variable]].get)(helpers.as3_5(inp.position))

        case (mp: astV3_3.MapProjection, children: Seq[AnyRef]) =>
          expressionsv3_5.MapProjection(children(0).asInstanceOf[expressionsv3_5.Variable],
            children(1).asInstanceOf[Seq[expressionsv3_5.MapProjectionElement]])(helpers.as3_5(mp.position))

        case (item@(_: compilerV3_3.ast.PrefixSeekRangeWrapper |
                    _: compilerV3_3.ast.InequalitySeekRangeWrapper |
                    _: compilerV3_3.ast.NestedPlanExpression |
                    _: compilerV3_3.ast.ResolvedFunctionInvocation), children: Seq[AnyRef]) =>
          convertVersion("compiler.v3_3.ast", "v3_5.logical.plans", item, children, helpers.as3_5(item.asInstanceOf[astV3_3.ASTNode].position), classOf[InputPosition])

        case (item: astV3_3.rewriters.DesugaredMapProjection, children: Seq[AnyRef]) =>
          convertVersion("org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters", "org.opencypher.v9_0.expressions", item, children, helpers.as3_5(item.position), classOf[InputPosition])

        case (item: astV3_3.ProcedureResultItem, children: Seq[AnyRef]) =>
          convertVersion(v3_3_AST, "org.opencypher.v9_0.ast", item, children, helpers.as3_5(item.position), classOf[InputPosition])

        case (item: plansV3_3.ResolvedCall, children: Seq[AnyRef]) =>
          convertVersion("org.neo4j.cypher.internal.v3_3.logical.plans", "org.neo4j.cypher.internal.v3_5.logical.plans", item, children, helpers.as3_5(item.position), classOf[InputPosition])

        case (expressionV3_3: astV3_3.ASTNode, children: Seq[AnyRef]) =>
          convertVersion(v3_3_AST, "org.opencypher.v9_0.expressions", expressionV3_3, children, helpers.as3_5(expressionV3_3.position), classOf[InputPosition])

        case (symbolsV3_3.CTAny, _) => symbolsv3_5.CTAny
        case (symbolsV3_3.CTBoolean, _) => symbolsv3_5.CTBoolean
        case (symbolsV3_3.CTFloat, _) => symbolsv3_5.CTFloat
        case (symbolsV3_3.CTGeometry, _) => symbolsv3_5.CTGeometry
        case (symbolsV3_3.CTGraphRef, _) => symbolsv3_5.CTGraphRef
        case (symbolsV3_3.CTInteger, _) => symbolsv3_5.CTInteger
        case (symbolsV3_3.ListType(_), children: Seq[AnyRef]) => symbolsv3_5.CTList(children.head.asInstanceOf[symbolsv3_5.CypherType])
        case (symbolsV3_3.CTMap, _) => symbolsv3_5.CTMap
        case (symbolsV3_3.CTNode, _) => symbolsv3_5.CTNode
        case (symbolsV3_3.CTNumber, _) => symbolsv3_5.CTNumber
        case (symbolsV3_3.CTPath, _) => symbolsv3_5.CTPath
        case (symbolsV3_3.CTPoint, _) => symbolsv3_5.CTPoint
        case (symbolsV3_3.CTRelationship, _) => symbolsv3_5.CTRelationship
        case (symbolsV3_3.CTString, _) => symbolsv3_5.CTString

        case (SemanticDirectionV3_3.BOTH, _) => expressionsv3_5.SemanticDirection.BOTH
        case (SemanticDirectionV3_3.INCOMING, _) => expressionsv3_5.SemanticDirection.INCOMING
        case (SemanticDirectionV3_3.OUTGOING, _) => expressionsv3_5.SemanticDirection.OUTGOING

        case (irV3_3.SimplePatternLength, _) => irv3_5.SimplePatternLength

        case (plansV3_3.IncludeTies, _) => plansv3_5.IncludeTies
        case (plansV3_3.DoNotIncludeTies, _) => plansv3_5.DoNotIncludeTies

        case (irV3_3.HasHeaders, _) => irv3_5.HasHeaders
        case (irV3_3.NoHeaders, _) => irv3_5.NoHeaders

        case (plansV3_3.ExpandAll, _) => plansv3_5.ExpandAll
        case (plansV3_3.ExpandInto, _) => plansv3_5.ExpandInto

        case (_: frontendV3_3.ExhaustiveShortestPathForbiddenException, _) => new utilv3_5.ExhaustiveShortestPathForbiddenException

        case (spp: irV3_3.ShortestPathPattern, children: Seq[AnyRef]) =>
          val sp3_4 = convertASTNode[expressionsv3_5.ShortestPaths](spp.expr, expressionMap, solveds, cardinalities, ids, isImportant)
          irv3_5.ShortestPathPattern(children(0).asInstanceOf[Option[String]], children(1).asInstanceOf[irv3_5.PatternRelationship], children(2).asInstanceOf[Boolean])(sp3_4)

        case (astV3_3.NilPathStep, _) => expressionsv3_5.NilPathStep

        case (item@(_: astV3_3.PathStep | _: astV3_3.NameToken[_]), children: Seq[AnyRef]) =>
          convertVersion(v3_3_AST, "org.opencypher.v9_0.expressions", item, children)

        case (nameId: frontendV3_3.NameId, children: Seq[AnyRef]) =>
          convertVersion("org.neo4j.cypher.internal.frontend.v3_3", "org.opencypher.v9_0.util", nameId, children)

        case (frontendV3_3.helpers.Fby(head, tail), children: Seq[AnyRef]) => utilv3_5.Fby(children(0), children(1).asInstanceOf[utilv3_5.NonEmptyList[_]])

        case (frontendV3_3.helpers.Last(head), children: Seq[AnyRef]) => utilv3_5.Last(children(0))

        case ( _:plansV3_3.ProcedureSignature, children: Seq[AnyRef]) =>
         plansv3_5.ProcedureSignature(children(0).asInstanceOf[QualifiedName],
                                      children(1).asInstanceOf[IndexedSeq[FieldSignature]],
                                      children(2).asInstanceOf[Option[IndexedSeq[FieldSignature]]],
                                      children(3).asInstanceOf[Option[String]],
                                      children(4).asInstanceOf[ProcedureAccessMode],
                                      children(5).asInstanceOf[Option[String]],
                                      children(6).asInstanceOf[Option[String]],
                                      None)

        case ( _:plansV3_3.UserFunctionSignature, children: Seq[AnyRef]) =>
          plansv3_5.UserFunctionSignature(children(0).asInstanceOf[QualifiedName],
                                       children(1).asInstanceOf[IndexedSeq[FieldSignature]],
                                       children(2).asInstanceOf[CypherType],
                                       children(3).asInstanceOf[Option[String]],
                                       children(4).asInstanceOf[Array[String]],
                                       children(5).asInstanceOf[Option[String]],
                                       children(6).asInstanceOf[Boolean],
                                       None)

        case (item@(_: plansV3_3.CypherValue |
                    _: plansV3_3.QualifiedName |
                    _: plansV3_3.FieldSignature |
                    _: plansV3_3.ProcedureAccessMode |
                    _: plansV3_3.QueryExpression[_] |
                    _: plansV3_3.SeekableArgs |
                    _: irV3_3.PatternRelationship |
                    _: irV3_3.VarPatternLength |
                    _: plansV3_3.ColumnOrder), children: Seq[AnyRef]) =>
          convertVersion("v3_3", "v3_5", item, children)

        case (item: frontendV3_3.Bound[_], children: Seq[AnyRef]) =>
          convertVersion("frontend.v3_3", "v3_5.logical.plans", item, children)

        case (item: compilerV3_3.SeekRange[_], children: Seq[AnyRef]) =>
          convertVersion("compiler.v3_3", "v3_5.logical.plans", item, children)
      }.apply(before)

      before._1 match {
        case plan: LogicalPlanV3_3 =>
          try {
            val plan3_4 = rewritten.asInstanceOf[LogicalPlanv3_5]
            // Set other attributes that were part of the plan in 3.3
            solveds.set(plan3_4.id, new PlannerQueryWrapper(plan.solved))
            cardinalities.set(plan3_4.id, helpers.as3_5(plan.solved.estimatedCardinality))
          } catch {
            case (_: frontendV3_3.InternalException) =>
            // ProcedureOrSchema plans have no assigned IDs. That's ok.
          }
        // Save Mapping from 3.3 expression to 3.5 expression
        case e: ExpressionV3_3 if isImportant(e) => expressionMap += (((e, e.position), rewritten.asInstanceOf[Expressionv3_5]))
        case _ =>
      }
      rewritten
    }
  }

  def convertLogicalPlan[T <: LogicalPlanv3_5](logicalPlan: LogicalPlanV3_3,
                                               solveds: Solveds,
                                               cardinalities: Cardinalities,
                                               idConverter: IdConverter,
                                               isImportant: ExpressionV3_3 => Boolean = _ => true): (LogicalPlanv3_5, ExpressionMapping3To4) = {
    val rewriter = new LogicalPlanRewriter(solveds, cardinalities, idConverter, isImportant = isImportant)
    val planv3_5 = new RewritableAny[LogicalPlanV3_3](logicalPlan).rewrite(rewriter, Seq.empty).asInstanceOf[T]
    (planv3_5, rewriter.expressionMap.toMap)
  }

  private[v3_3] def convertExpression[T <: Expressionv3_5](expression: ExpressionV3_3,
                                                           solveds: Solveds,
                                                           cardinalities: Cardinalities,
                                                           idConverter: IdConverter): T = {
    new RewritableAny[ExpressionV3_3](expression)
      .rewrite(new LogicalPlanRewriter(solveds, cardinalities, idConverter), Seq.empty)
      .asInstanceOf[T]
  }

  private def convertASTNode[T <: utilv3_5.ASTNode](ast: astV3_3.ASTNode,
                                                    expressionMap: MutableExpressionMapping3To4,
                                                    solveds: Solveds,
                                                    cardinalities: Cardinalities,
                                                    idConverter: IdConverter,
                                                    isImportant: ExpressionV3_3 => Boolean): T = {
    new RewritableAny[astV3_3.ASTNode](ast)
      .rewrite(new LogicalPlanRewriter(solveds,
                                       cardinalities,
                                       idConverter,
                                       expressionMap,
                                       isImportant), Seq.empty)
      .asInstanceOf[T]
  }

  private val constructors = new ThreadLocal[MutableHashMap[(String, String, String), Constructor[_]]]() {
    override def initialValue: MutableHashMap[(String, String, String), Constructor[_]] =
      new MutableHashMap[(String, String, String), Constructor[_]]
  }

  private def getConstructor(classNameV3_3: String, oldPackage: String, newPackage: String): Constructor[_] = {
    constructors.get.getOrElseUpdate((classNameV3_3, oldPackage, newPackage), {
      assert(classNameV3_3.contains(oldPackage), s"wrong 3.3 package name given. $classNameV3_3 does not contain $oldPackage")
      val classNamev3_5 = classNameV3_3.replace(oldPackage, newPackage)
      Try(Class.forName(classNamev3_5)).map(_.getConstructors.head) match {
        case Success(c) => c
        case Failure(e: ClassNotFoundException) => throw new InternalException(
          s"Failed trying to rewrite $classNameV3_3 - 3.5 class not found ($classNamev3_5)", e)
        case Failure(e: NoSuchElementException) => throw new InternalException(
          s"Failed trying to rewrite $classNameV3_3 - this class does not have a constructor", e)
        case Failure(e) => throw e
      }
    })
  }

  private def convertVersion(oldPackage: String,
                             newPackage: String,
                             thing: AnyRef,
                             children: Seq[AnyRef],
                             extraArg: AnyRef = null,
                             assignableClazzForArg: Class[_] = null,
                             extraArg2: AnyRef = null,
                             assignableClazzForArg2: Class[_] = null): AnyRef = {
    val thingClass = thing.getClass
    val classNameV3_3 = thingClass.getName
    val constructor = getConstructor(classNameV3_3, oldPackage, newPackage)

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
    * Converts a 3.3 CreateNode or CreateRelationship logical plan operator into a 3.5 Create operator.
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
