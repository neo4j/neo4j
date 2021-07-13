/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans._
import org.neo4j.cypher.internal.ir.v3_5.{InterestingOrder, PatternRelationship, QueryGraph}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, SeekableArgs}
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.NodeNameGenerator

object idSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val arguments: Set[LogicalVariable] = qg.argumentIds.map(n => Variable(n)(null))
    val idSeekPredicates: Option[(Expression, LogicalVariable, SeekableArgs)] = e match {
      // MATCH (a)-[r]-(b) WHERE id(r) IN expr
      // MATCH a WHERE id(a) IN {param}
      case predicate@AsIdSeekable(seekable) if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
        Some((predicate, seekable.ident, seekable.args))
      case _ => None
    }

    idSeekPredicates map {
      case (predicate, idExpr@Variable(id), idValues) if !qg.argumentIds.contains(id) =>

        qg.patternRelationships.find(_.name == id) match {
          case Some(relationship) =>
            val startNodeAndEndNodeIsSame = relationship.left == relationship.right
            val startOrEndNodeIsBound = relationship.coveredIds.intersect(qg.argumentIds).nonEmpty

            val types = relationship.types.toList

            if (!startNodeAndEndNodeIsSame) {
              val types = relationship.types.toList
              val seekPlan = planRelationshipByIdSeek(relationship, relationship.nodes, idValues, Seq(predicate), qg.argumentIds, context)
              LeafPlansForVariable(id, Set(planRelTypeFilter(seekPlan, idExpr, types, context)))
            } else if (startOrEndNodeIsBound) {
              // if start/end node variables are already bound, generate new variable names and plan a Selection after the seek
              val oldNodes = relationship.nodes
              val newNodes = generateNewStartEndNodes(oldNodes, qg.argumentIds, idExpr.position)
              // For a pattern (a)-[r]-(b), nodePredicates will be something like `a = new_a AND b = new_B`,
              // where `new_a` and `new_b` are the newly generated variable names.
              // This case covers the scenario where (a)-[r]-(a), because the nodePredicate `a = new_a1 AND a = new_a2` implies `new_a1 = new_a2`
              val nodePredicates = buildNodePredicates(oldNodes, newNodes)

              val seekPlan = planRelationshipByIdSeek(relationship, newNodes, idValues, Seq(predicate), qg.argumentIds, context)
              val relTypeSelectionPlan = planRelTypeFilter(seekPlan, idExpr, types, context)
              val nodesSelectionPlan = context.logicalPlanProducer.planHiddenSelection(nodePredicates, relTypeSelectionPlan, context)
              LeafPlansForVariable(id, Set(nodesSelectionPlan))
            } else {
              // In the case where `startNodeAndEndNodeIsSame == true` we need to generate 1 new variable name for one side of the relationship
              // and plan a Selection after the seek so that both sides are the same
              val newRightNode = NodeNameGenerator.name(idExpr.position.bumped())
              val nodePredicate = equalsPredicate(relationship.right, newRightNode)
              val seekPlan = planRelationshipByIdSeek(relationship, (relationship.left, newRightNode), idValues, Seq(predicate), qg.argumentIds, context)
              val relTypeSelectionPlan = planRelTypeFilter(seekPlan, idExpr, types, context)
              val nodesSelectionPlan = context.logicalPlanProducer.planHiddenSelection(Seq(nodePredicate), relTypeSelectionPlan, context)
              LeafPlansForVariable(id, Set(nodesSelectionPlan))
            }

          case None =>
            val plan = context.logicalPlanProducer.planNodeByIdSeek(id, idValues, Seq(predicate), qg.argumentIds, context)
            LeafPlansForVariable(id, Set(plan))
        }
    }
  }

  override def apply(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] =
    queryGraph.selections.flatPredicates.flatMap(e => producePlanFor(e, queryGraph, interestingOrder, context).toSeq.flatMap(_.plans))

  private def planRelationshipByIdSeek(relationship: PatternRelationship, nodes: (String, String), idValues: SeekableArgs, predicates: Seq[Expression], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val (left, right) = nodes
    val name = relationship.name
    relationship.dir match {
      case BOTH     => context.logicalPlanProducer.planUndirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates, context)
      case INCOMING => context.logicalPlanProducer.planDirectedRelationshipByIdSeek(name, idValues, right, left, relationship, argumentIds, predicates, context)
      case OUTGOING => context.logicalPlanProducer.planDirectedRelationshipByIdSeek(name, idValues, left, right, relationship, argumentIds, predicates, context)
    }
  }

  /**
   * Generate new variable names for start and end node, but only for those nodes that are arguments.
   * Otherwise, return the same variable name.
   */
  private def generateNewStartEndNodes(oldNodes: (String, String),
                                       argumentIds: Set[String],
                                       pos: InputPosition) = {
    val (left, right) = oldNodes
    val newLeft = if (!argumentIds.contains(left)) left else NodeNameGenerator.name(pos.bumped())
    val newRight = if (!argumentIds.contains(right)) right else NodeNameGenerator.name(pos.bumped().bumped())
    (newLeft, newRight)
  }

  private def buildNodePredicates(oldNodes: (String, String), newNodes: (String, String)) = {
    def pred(oldName: String, newName: String) = if (oldName == newName) Seq.empty else Seq(equalsPredicate(oldName, newName))

    val (oldLeft, oldRight) = oldNodes
    val (newLeft, newRight) = newNodes

    pred(oldLeft, newLeft) ++ pred(oldRight, newRight)
  }

  private def equalsPredicate(left: String, right: String) = {
    val pos = InputPosition.NONE
    Equals(
      Variable(left)(pos),
      Variable(right)(pos)
    )(pos)
  }

  private def planRelTypeFilter(plan: LogicalPlan, idExpr: Variable, relTypes: List[RelTypeName], context: LogicalPlanningContext): LogicalPlan = {
    relTypes match {
      case Seq(tpe) =>
        val relTypeExpr = relTypeAsStringLiteral(tpe)
        val predicate = Equals(typeOfRelExpr(idExpr), relTypeExpr)(idExpr.position)
        context.logicalPlanProducer.planHiddenSelection(Seq(predicate), plan, context)

      case tpe :: _ =>
        val relTypeExprs = relTypes.map(relTypeAsStringLiteral).toSet
        val invocation = typeOfRelExpr(idExpr)
        val idPos = idExpr.position
        val predicate = Ors(relTypeExprs.map { expr => Equals(invocation, expr)(idPos) } )(idPos)
        context.logicalPlanProducer.planHiddenSelection(Seq(predicate), plan, context)

      case _ =>
        plan
    }
  }

  private def relTypeAsStringLiteral(relType: RelTypeName) = StringLiteral(relType.name)(relType.position)

  private def typeOfRelExpr(idExpr: Variable) =
    FunctionInvocation(FunctionName("type")(idExpr.position), idExpr)(idExpr.position)
}
