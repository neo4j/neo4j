/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.ast.{InequalitySeekRangeWrapper, PrefixSeekRangeWrapper}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_3.ast.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, ast}
import org.neo4j.cypher.internal.ir.v3_3.IdName

object LogicalPlan2PlanDescription extends ((LogicalPlan, Map[LogicalPlan, Id]) => InternalPlanDescription) {
  override def apply(input: LogicalPlan, idMap: Map[LogicalPlan, Id]): InternalPlanDescription = {
    val readOnly = input.solved.readOnly
    new LogicalPlan2PlanDescription(idMap, readOnly).create(input)
  }
}

case class LogicalPlan2PlanDescription(idMap: Map[LogicalPlan, Id], readOnly: Boolean) extends TreeBuilder[InternalPlanDescription] {
  override protected def build(plan: LogicalPlan): InternalPlanDescription = {
    assert(plan.isLeaf)

    val id = idMap(plan)
    val variables = plan.availableSymbols.map(_.name)

    val result: InternalPlanDescription = plan match {
      case _: AllNodesScan =>
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq.empty, variables)

      case _: plans.Argument =>
        PlanDescriptionImpl(id, "Argument", NoChildren, Seq.empty, variables)

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

      case _: SingleRow =>
        SingleRowPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(_, relIds, _, _, _) =>
        val entityByIdRhs = EntityByIdRhs(relIds)
        PlanDescriptionImpl(id, "DirectedRelationshipByIdSeekPipe", NoChildren, Seq(entityByIdRhs), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", NoChildren, Seq.empty, variables)

      case NodeCountFromCountStore(IdName(variable), labelNames, _) =>
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

      case RelationshipCountFromCountStore(IdName(ident), startLabel, typeNames, endLabel, _) =>
        val exp = CountRelationshipsExpression(ident, startLabel.map(_.name), typeNames.map(_.name), endLabel.map(_.name))
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(exp), variables)

      case _: UndirectedRelationshipByIdSeek =>
        PlanDescriptionImpl(id, "UndirectedRelationshipByIdSeek", NoChildren, Seq.empty, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    result.addArgument(EstimatedRows(plan.solved.estimatedCardinality.amount))
  }

  override protected def build(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    assert(plan.lhs.nonEmpty)
    assert(plan.rhs.isEmpty)

    val id = idMap(plan)
    val variables = plan.availableSymbols.map(_.name)
    val children = if (source.isInstanceOf[SingleRowPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case Aggregation(_, groupingExpressions, aggregationExpressions) if aggregationExpressions.isEmpty =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Aggregation(_, groupingExpressions, _) =>
        PlanDescriptionImpl(id, "EagerAggregation", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case _: CreateNode =>
        PlanDescriptionImpl(id, "CreateNode", children, Seq.empty, variables)

      case _: CreateRelationship =>
        PlanDescriptionImpl(id, "CreateRelationship", children, Seq.empty, variables)

      case _: DeleteExpression | _: DeleteNode | _: DeletePath | _: DeleteRelationship =>
        PlanDescriptionImpl(id, "Delete", children, Seq.empty, variables)

      case _: DetachDeleteExpression | _: DetachDeleteNode | _: DetachDeletePath =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq.empty, variables)

      case _: Eager =>
        PlanDescriptionImpl(id, "Eager", children, Seq.empty, variables)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables)
      case NodeCountFromCountStore(IdName(id), labelName, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeCountFromCountStore", NoChildren,
                            Seq(CountNodesExpression(id, labelName.map(l => l.map(_.name)))), variables)

      case RelationshipCountFromCountStore(IdName(id), start, types, end, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "RelationshipCountFromCountStore", NoChildren,
                            Seq(CountRelationshipsExpression(id, start.map(_.name), types.map(_.name), end.map(_.name))),
                            variables)

      case NodeUniqueIndexSeek(IdName(id), label, propKeys, value, arguments) =>
        PlanDescriptionImpl(id = idMap(plan), "NodeUniqueIndexSeek", NoChildren, Seq(Index(label.name, propKeys.map(_.name))), variables)

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables)

      case Expand(_, IdName(fromName), dir, typeNames, IdName(toName), IdName(relName), mode) =>
        val expression = ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(expression), variables)

      case Limit(_, count, DoNotIncludeTies) =>
        PlanDescriptionImpl(id, name = "Limit", children, Seq(Expression(count)), variables)

      case LockNodes(_, nodesToLock) =>
        PlanDescriptionImpl(id, name = "LockNodes", children, Seq(KeyNames(nodesToLock.map(_.name).toSeq)), variables)

      case OptionalExpand(_, IdName(fromName), dir, typeNames, IdName(toName), IdName(relName), mode, predicates) =>
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
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq(Expression(count)), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", children, Seq.empty, variables)

      case _: MergeCreateNode =>
        PlanDescriptionImpl(id, "MergeCreateNode", children, Seq.empty, variables)

      case _: MergeCreateRelationship =>
        PlanDescriptionImpl(id, "MergeCreateRelationship", children, Seq.empty, variables)

      case _: Optional =>
        PlanDescriptionImpl(id, "Optional", children, Seq.empty, variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", children, Seq(signature), variables)

      case ProjectEndpoints(_, IdName(relName), IdName(start), _, IdName(end), _, _, directed, _) =>
        val name = if (directed) "ProjectEndpoints" else "ProjectEndpoints(BOTH)"
        PlanDescriptionImpl(id, name, children, Seq(KeyNames(Seq(relName, start, end))), variables)

      case PruningVarExpand(_, IdName(fromName), dir, types, IdName(toName), min, max, predicates) =>
        val expandSpec = ExpandExpression(fromName, "", types.map(_.name), toName, dir, minLength = min, maxLength = Some(max))
        PlanDescriptionImpl(id, s"VarLengthExpand(Pruning)", children, Seq(expandSpec), variables)

      case FullPruningVarExpand(_, IdName(fromName), dir, types, IdName(toName), min, max, predicates) =>
        val expandSpec = ExpandExpression(fromName, "", types.map(_.name), toName, dir, minLength = min, maxLength = Some(max))
        PlanDescriptionImpl(id, s"VarLengthExpand(FullPruning)", children, Seq(expandSpec), variables)

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
        PlanDescriptionImpl(id, "Sort", children, Seq(KeyNames(orderBy.map(_.id.name))), variables)

      case Top(_, orderBy, limit) =>
        PlanDescriptionImpl(id, "Top", children, Seq(KeyNames(orderBy.map(_.id.name)), Expression(limit)), variables)

      case UnwindCollection(_, _, expression) =>
        PlanDescriptionImpl(id, "Unwind", children, Seq(Expression(expression)), variables)

      case VarExpand(_, IdName(fromName), dir, _, types, IdName(toName), IdName(relName), length, mode, predicates) =>
        val expandDescription = ExpandExpression(fromName, relName, types.map(_.name), toName, dir, minLength = length.min, maxLength = length.max)
        val predicatesMap = predicates.map(_._2).zipWithIndex.map({ case (p, idx) => s"p$idx" -> p }).toMap
        val predicatesDescription = if (predicatesMap.isEmpty)
          None
        else
          Some(Expressions(predicatesMap))
        val modeDescr = mode match {
          case ExpandAll => "All"
          case ExpandInto => "Into"
        }
        PlanDescriptionImpl(id, s"VarLengthExpand($modeDescr)", children, Seq(expandDescription) ++ predicatesDescription, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    result.addArgument(EstimatedRows(plan.solved.estimatedCardinality.amount))
  }

  override protected def build(plan: LogicalPlan, lhs: InternalPlanDescription, rhs: InternalPlanDescription): InternalPlanDescription = {
    assert(plan.lhs.nonEmpty)
    assert(plan.rhs.nonEmpty)

    val id = idMap(plan)
    val variables = plan.availableSymbols.map(_.name)
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
        PlanDescriptionImpl(id, "NodeHashJoin", children, Seq(KeyNames(nodes.toIndexedSeq.map(_.name))), variables)

      case _: ForeachApply =>
        PlanDescriptionImpl(id, "Foreach", children, Seq.empty, variables)

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case row: SingleRow =>
        SingleRowPlanDescription(id = idMap(plan), Seq.empty, row.argumentIds.map(_.name))

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case _: LetSemiApply =>
        PlanDescriptionImpl(id, "LetSemiApply", children, Seq.empty, variables)

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq.empty, variables)

      case OuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeOuterHashJoin", children, Seq(KeyNames(nodes.map(_.name).toSeq)), variables)

      case RollUpApply(_, _, collectionName, _, _) =>
        PlanDescriptionImpl(id, "RollUpApply", children, Seq(KeyNames(Seq(collectionName.name))), variables)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables)

      case TriadicSelection(_, _, IdName(source), IdName(seen), IdName(target), _) =>
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

    result.addArgument(EstimatedRows(plan.solved.estimatedCardinality.amount))
  }

  private def getDescriptions(label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[ast.Expression],
                              unique: Boolean,
                              readOnly: Boolean): (String, planDescription.Argument) = {

    val (name, indexDesc) = valueExpr match {
      case e: RangeQueryExpression[_]  =>
        assert(propertyKeys.size == 1, "Range queries not yet supported for composite indexes")
        val propertyKey = propertyKeys.head.name
        val name = if (unique) "NodeUniqueIndexSeekByRange" else "NodeIndexSeekByRange"
        e.expression match {
          case PrefixSeekRangeWrapper(range) =>
            (name, PrefixIndex(label.name, propertyKey, range.prefix))
          case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey, bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq))
          case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey, bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq))
          case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
            val greaterThanBoundsText = greaterThanBounds.bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq
            val lessThanBoundsText = lessThanBounds.bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq
            (name, InequalityIndex(label.name, propertyKey, greaterThanBoundsText ++ lessThanBoundsText))
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
