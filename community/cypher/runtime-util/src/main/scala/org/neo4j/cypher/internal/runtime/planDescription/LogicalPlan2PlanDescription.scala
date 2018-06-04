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
package org.neo4j.cypher.internal.runtime.planDescription

import org.opencypher.v9_0.frontend.PlannerName
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.opencypher.v9_0.util.InternalException
import org.opencypher.v9_0.expressions.{FunctionInvocation, FunctionName, LabelToken, MapExpression, Namespace, PropertyKeyToken, Expression => ASTExpression}
import org.opencypher.v9_0.expressions.functions.Point
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans._

object LogicalPlan2PlanDescription extends ((LogicalPlan, PlannerName, Boolean, Cardinalities) => InternalPlanDescription) {

  override def apply(input: LogicalPlan,
                     plannerName: PlannerName,
                     readOnly: Boolean,
                     cardinalities: Cardinalities): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(readOnly, cardinalities).create(input)
      .addArgument(Version("CYPHER "+plannerName.version))
      .addArgument(RuntimeVersion("3.5"))
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersion(plannerName.version))
  }
}

case class LogicalPlan2PlanDescription(readOnly: Boolean, cardinalities: Cardinalities)
  extends TreeBuilder[InternalPlanDescription] {

  override protected def build(plan: LogicalPlan): InternalPlanDescription = {
    assert(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols

    val result: InternalPlanDescription = plan match {
      case _: AllNodesScan =>
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq.empty, variables)

      case NodeByLabelScan(_, label, _) =>
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(LabelName(label.name)), variables)

      case NodeByIdSeek(_, _, _) =>
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(), variables)

      case NodeIndexSeek(_, label, propertyKeys, valueExpr, _) =>
        val (indexMode, indexDesc) = getDescriptions(label, propertyKeys, valueExpr, unique = false, readOnly)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(indexDesc), variables)

      case NodeUniqueIndexSeek(_, label, propertyKeys, valueExpr, _) =>
        val (indexMode, indexDesc) = getDescriptions(label, propertyKeys, valueExpr, unique = true, readOnly)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(indexDesc), variables)

      case ProduceResult(_, _) =>
        PlanDescriptionImpl(id, "ProduceResults", NoChildren, Seq(), variables)

      case _: plans.Argument if variables.nonEmpty =>
        PlanDescriptionImpl(id, "Argument", NoChildren, Seq.empty, variables)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(_, relIds, _, _, _) =>
        val entityByIdRhs = EntityByIdRhs(relIds)
        PlanDescriptionImpl(id, "DirectedRelationshipByIdSeek", NoChildren, Seq(entityByIdRhs), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", NoChildren, Seq.empty, variables)

      case NodeCountFromCountStore(variable, labelNames, _) =>
        val arguments = Seq(CountNodesExpression(variable, labelNames.map(l => l.map(_.name))))
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, arguments, variables)

      case NodeIndexContainsScan(_, label, propertyKey, valueExpr, _) =>
        val arguments = Seq(Index(label.name, Seq(propertyKey.name)), Expression(valueExpr))
        PlanDescriptionImpl(id, "NodeIndexContainsScan", NoChildren, arguments, variables)

      case NodeIndexEndsWithScan(_, label, propertyKey, valueExpr, _) =>
        val arguments = Seq(Index(label.name, Seq(propertyKey.name)), Expression(valueExpr))
        PlanDescriptionImpl(id, "NodeIndexEndsWithScan", NoChildren, arguments, variables)

      case NodeIndexScan(_, label, propertyKey, _) =>
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren, Seq(Index(label.name, Seq(propertyKey.name))), variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", NoChildren, Seq(signature), variables)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val exp = CountRelationshipsExpression(ident, startLabel.map(_.name), typeNames.map(_.name),
                                               endLabel.map(_.name))
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(exp), variables)

      case _: UndirectedRelationshipByIdSeek =>
        PlanDescriptionImpl(id, "UndirectedRelationshipByIdSeek", NoChildren, Seq.empty, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    result.addArgument(EstimatedRows(cardinalities.get(plan.id).amount))
  }

  override protected def build(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    assert(plan.lhs.nonEmpty)
    assert(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols
    val children = if (source.isInstanceOf[ArgumentPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case Aggregation(_, groupingExpressions, aggregationExpressions) if aggregationExpressions.isEmpty =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Aggregation(_, groupingExpressions, _) =>
        PlanDescriptionImpl(id, "EagerAggregation", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)),
                            variables)

      case _: Create =>
        PlanDescriptionImpl(id, "Create", children, Seq.empty, variables)

      case _: DeleteExpression | _: DeleteNode | _: DeletePath | _: DeleteRelationship =>
        PlanDescriptionImpl(id, "Delete", children, Seq.empty, variables)

      case _: DetachDeleteExpression | _: DetachDeleteNode | _: DetachDeletePath =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq.empty, variables)

      case _: Eager =>
        PlanDescriptionImpl(id, "Eager", children, Seq.empty, variables)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables)

      case _: DropResult =>
        PlanDescriptionImpl(id, "DropResult", children, Seq.empty, variables)

      case NodeCountFromCountStore(id, labelName, arguments) =>
        PlanDescriptionImpl(id = plan.id, "NodeCountFromCountStore", NoChildren,
                            Seq(CountNodesExpression(id, labelName.map(l => l.map(_.name)))), variables)

      case RelationshipCountFromCountStore(id, start, types, end, arguments) =>
        PlanDescriptionImpl(id = plan.id, "RelationshipCountFromCountStore", NoChildren,
                            Seq(
                              CountRelationshipsExpression(id, start.map(_.name), types.map(_.name), end.map(_.name))),
                            variables)

      case NodeUniqueIndexSeek(id, label, propKeys, value, arguments) =>
        PlanDescriptionImpl(id = plan.id, "NodeUniqueIndexSeek", NoChildren,
                            Seq(Index(label.name, propKeys.map(_.name))), variables)

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode) =>
        val expression = ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(expression), variables)

      case Limit(_, count, DoNotIncludeTies) =>
        PlanDescriptionImpl(id, name = "Limit", children, Seq(Expression(count)), variables)

      case LockNodes(_, nodesToLock) =>
        PlanDescriptionImpl(id, name = "LockNodes", children, Seq(KeyNames(nodesToLock.toSeq)), variables)

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates) =>
        val expressions = predicates.map(Expression.apply) :+
          ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, expressions, variables)

      case ProduceResult(_, _) =>
        PlanDescriptionImpl(id, "ProduceResults", children, Seq(), variables)

      case Projection(_, expr) =>
        val expressions = Expressions(expr)
        PlanDescriptionImpl(id, "Projection", children, Seq(expressions), variables)

      case Selection(predicates, _) =>
        PlanDescriptionImpl(id, "Filter", children, predicates.map(Expression), variables)

      case Skip(_, count) =>
        PlanDescriptionImpl(id, name = "Skip", children, Seq(Expression(count)), variables)

      case FindShortestPaths(_, _, predicates, _, _) =>
        val args = predicates.zipWithIndex.map { case (p, idx) => s"p$idx" -> p }
        PlanDescriptionImpl(id, "ShortestPath", children, Seq(Expressions(args.toMap)), variables)

      case Limit(_, count, _) =>
        PlanDescriptionImpl(id, "Limit", children, Seq(Expression(count)), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", children, Seq.empty, variables)

      case _: MergeCreateNode =>
        PlanDescriptionImpl(id, "MergeCreateNode", children, Seq.empty, variables)

      case _: MergeCreateRelationship =>
        PlanDescriptionImpl(id, "MergeCreateRelationship", children, Seq.empty, variables)

      case _: ActiveRead =>
        PlanDescriptionImpl(id, "ActiveRead", children, Seq.empty, variables)

      case _: Optional =>
        PlanDescriptionImpl(id, "Optional", children, Seq.empty, variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", children, Seq(signature), variables)

      case ProjectEndpoints(_, relName, start, _, end, _, _, directed, _) =>
        val name = if (directed) "ProjectEndpoints" else "ProjectEndpoints(BOTH)"
        PlanDescriptionImpl(id, name, children, Seq(KeyNames(Seq(relName, start, end))), variables)

      case PruningVarExpand(_, fromName, dir, types, toName, min, max, predicates) =>
        val expandSpec = ExpandExpression(fromName, "", types.map(_.name), toName, dir, minLength = min,
                                          maxLength = Some(max))
        PlanDescriptionImpl(id, s"VarLengthExpand(Pruning)", children, Seq(expandSpec), variables)

      case _: RemoveLabels =>
        PlanDescriptionImpl(id, "RemoveLabels", children, Seq.empty, variables)

      case _: SetLabels =>
        PlanDescriptionImpl(id, "SetLabels", children, Seq.empty, variables)

      case _: SetNodePropertiesFromMap =>
        PlanDescriptionImpl(id, "SetNodePropertyFromMap", children, Seq.empty, variables)

      case _: SetProperty |
           _: SetNodeProperty |
           _: SetRelationshipPropery =>
        PlanDescriptionImpl(id, "SetProperty", children, Seq.empty, variables)

      case _: SetRelationshipPropertiesFromMap =>
        PlanDescriptionImpl(id, "SetRelationshipPropertyFromMap", children, Seq.empty, variables)

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(id, "Sort", children, Seq(KeyNames(orderBy.map(_.id))), variables)

      case Top(_, orderBy, limit) =>
        PlanDescriptionImpl(id, "Top", children, Seq(KeyNames(orderBy.map(_.id)), Expression(limit)), variables)

      case UnwindCollection(_, _, expression) =>
        PlanDescriptionImpl(id, "Unwind", children, Seq(Expression(expression)), variables)

      case VarExpand(_, fromName, dir, _, types, toName, relName, length, mode, _, _, _, _, predicates) =>
        val expandDescription = ExpandExpression(fromName, relName, types.map(_.name), toName, dir,
                                                 minLength = length.min, maxLength = length.max)
        val predicatesMap = predicates.map(_._2).zipWithIndex.map({ case (p, idx) => s"p$idx" -> p }).toMap
        val predicatesDescription = if (predicatesMap.isEmpty)
          None
        else
          Some(Expressions(predicatesMap))
        val modeDescr = mode match {
          case ExpandAll => "All"
          case ExpandInto => "Into"
        }
        PlanDescriptionImpl(id, s"VarLengthExpand($modeDescr)", children,
                            Seq(expandDescription) ++ predicatesDescription, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    result.addArgument(EstimatedRows(cardinalities.get(plan.id).amount))
  }

  override protected def build(plan: LogicalPlan, lhs: InternalPlanDescription,
                               rhs: InternalPlanDescription): InternalPlanDescription = {
    assert(plan.lhs.nonEmpty)
    assert(plan.rhs.nonEmpty)

    val id = plan.id
    val variables = plan.availableSymbols
    val children = TwoChildren(lhs, rhs)

    val result: InternalPlanDescription = plan match {
      case _: AntiConditionalApply =>
        PlanDescriptionImpl(id, "AntiConditionalApply", children, Seq.empty, variables)

      case _: AntiSemiApply =>
        PlanDescriptionImpl(id, "AntiSemiApply", children, Seq.empty, variables)

      case _: ConditionalApply =>
        PlanDescriptionImpl(id, "ConditionalApply", children, Seq.empty, variables)

      case _: Apply =>
        PlanDescriptionImpl(id, "Apply", children, Seq.empty, variables)

      case _: AssertSameNode =>
        PlanDescriptionImpl(id, "AssertSameNode", children, Seq.empty, variables)

      case CartesianProduct(_, _) =>
        PlanDescriptionImpl(id, "CartesianProduct", children, Seq.empty, variables)

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeHashJoin", children, Seq(KeyNames(nodes.toIndexedSeq)), variables)

      case _: ForeachApply =>
        PlanDescriptionImpl(id, "Foreach", children, Seq.empty, variables)

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, row.argumentIds)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case _: LetSemiApply =>
        PlanDescriptionImpl(id, "LetSemiApply", children, Seq.empty, variables)

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq.empty, variables)

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeLeftOuterHashJoin", children, Seq(KeyNames(nodes.toSeq)), variables)

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeRightOuterHashJoin", children, Seq(KeyNames(nodes.toSeq)), variables)

      case RollUpApply(_, _, collectionName, _, _) =>
        PlanDescriptionImpl(id, "RollUpApply", children, Seq(KeyNames(Seq(collectionName))), variables)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables)

      case TriadicSelection(_, _, _, source, seen, target) =>
        PlanDescriptionImpl(id, "TriadicSelection", children, Seq(KeyNames(Seq(source, seen, target))), variables)

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Expression(predicate)),
          variables
        )

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    result.addArgument(EstimatedRows(cardinalities.get(plan.id).amount))
  }

  private def getDescriptions(label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[ASTExpression],
                              unique: Boolean,
                              readOnly: Boolean): (String, Argument) = {

    val (name, indexDesc) = valueExpr match {
      case e: RangeQueryExpression[_] =>
        assert(propertyKeys.size == 1, "Range queries not yet supported for composite indexes")
        val propertyKey = propertyKeys.head.name
        val name = if (unique) "NodeUniqueIndexSeekByRange" else "NodeIndexSeekByRange"
        e.expression match {
          case PrefixSeekRangeWrapper(range) =>
            (name, PrefixIndex(label.name, propertyKey, range.prefix))
          case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey,
              bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq))
          case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey,
              bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq))
          case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
            val greaterThanBoundsText = greaterThanBounds.bounds.map(bound =>
              s">${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq
            val lessThanBoundsText = lessThanBounds.bounds.map(bound =>
              s"<${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq
            (name, InequalityIndex(label.name, propertyKey, greaterThanBoundsText ++ lessThanBoundsText))
          case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
            val funcName = Point.name
            val poi = point match {
              case FunctionInvocation(Namespace(List()), FunctionName(funcName), _, Seq(MapExpression(args))) =>
                s"point(${args.map(_._1.name).mkString(",")})"
              case _ => point.toString
            }
            (name, PointDistanceIndex(label.name, propertyKey, poi, distance.toString, inclusive))
          case _ => throw new InternalException("This should never happen. Missing a case?")
        }
      case _ =>
        val name =
          if (unique && readOnly) "NodeUniqueIndexSeek"
          else if (unique) "NodeUniqueIndexSeek(Locking)"
          else "NodeIndexSeek"
        (name, Index(label.name, propertyKeys.map(_.name)))
    }

    (name, indexDesc)
  }
}
