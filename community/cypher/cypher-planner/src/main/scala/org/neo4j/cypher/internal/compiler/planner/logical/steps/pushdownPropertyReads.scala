package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalProperty, LogicalVariable, Property, Variable}
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attributes, Id}
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.internal.v4_0.util.{Cardinality, Rewriter, bottomUp}

import scala.collection.mutable

case object pushdownPropertyReads {

  def x(logicalPlan: LogicalPlan,
        cardinalities: Cardinalities,
        attributes: Attributes[LogicalPlan],
        semanticTable: SemanticTable): LogicalPlan = {

    def isNodeOrRel(variable: LogicalVariable): Boolean =
      semanticTable.types.get(variable)
        .exists(t => t.actual == CTNode.invariant || t.actual == CTRelationship.invariant)

    case class VarLowestCardinality(lowestCardinality: Cardinality, logicalPlanId: Id)
    case class Acc(variableOptima: Map[String, VarLowestCardinality],
                   propertyReadOptima: Seq[(Id, Property)],
                   availableProperties: Set[Property],
                   incomingCardinality: Cardinality)

    val Acc(_, propertyReadOptima, _, _) =
      LogicalPlans.foldPlan(Acc(Map.empty, Seq.empty, Set.empty, Cardinality.SINGLE))(
        logicalPlan,
        (acc, plan) => {
          val propertiesForPlan =
            plan.treeFold(List.empty[Property]) {
              case lp: LogicalPlan if lp.id != plan.id =>
                acc2 => (acc2, None) // do not traverse further
              case p @ Property(v: LogicalVariable, _) if isNodeOrRel(v) =>
                acc2 => (p :: acc2, Some(acc3 => acc3) )
            }

          val newPropertyReadOptima =
            propertiesForPlan.flatMap {
              case p @ Property(v: LogicalVariable, _) =>
                acc.variableOptima.get(v.name) match {
                  case Some(VarLowestCardinality(lowestCardinality, logicalPlanId)) =>
                    if (lowestCardinality < acc.incomingCardinality && !acc.availableProperties.contains(p))
                      Some((logicalPlanId, p))
                    else
                      None
                  // this happens for variables introduced in expressions, we ignore those for now
                  case None => None
                }

            }

          val outgoingCardinality = cardinalities(plan.id)
          val outgoingReadOptima = acc.propertyReadOptima ++ newPropertyReadOptima

          // TODO: handle aliasing in projections

          plan match {
            case _: Aggregation |
                 _: OrderedAggregation =>
              val newVariables = plan.availableSymbols
              val outgoingVariableOptima = newVariables.map(v => (v, VarLowestCardinality(outgoingCardinality, plan.id))).toMap

              Acc(outgoingVariableOptima, outgoingReadOptima, Set.empty, outgoingCardinality)
            case _ =>
              val newLowestCardinalities =
                acc.variableOptima.mapValues(x =>
                  if (outgoingCardinality < x.lowestCardinality) {
                    VarLowestCardinality(outgoingCardinality, plan.id)
                  } else {
                    x
                  }
                )

              val currentVariables = plan.availableSymbols
              val newVariables = currentVariables -- acc.variableOptima.keySet
              val newVariableCardinalities = newVariables.map(v => (v, VarLowestCardinality(outgoingCardinality, plan.id)))
              val outgoingVariableOptima = newLowestCardinalities ++ newVariableCardinalities

              Acc(outgoingVariableOptima, outgoingReadOptima, acc.availableProperties ++ propertiesForPlan, outgoingCardinality)
          }
        },
        (lhsAcc, rhsAcc, plan) => {
          val mergedVariableOptima =
            lhsAcc.variableOptima ++ rhsAcc.variableOptima.map {
              case (v, rhsOptimum) =>
                lhsAcc.variableOptima.get(v) match {
                  case Some(lhsOptimum) =>
                    (v, Seq(lhsOptimum, rhsOptimum).minBy(_.lowestCardinality))
                  case None =>
                    (v, rhsOptimum)
                }
            }

          Acc(
            mergedVariableOptima,
            lhsAcc.propertyReadOptima ++ rhsAcc.propertyReadOptima,
            lhsAcc.availableProperties ++ rhsAcc.availableProperties,
            cardinalities(plan.id))
        }
      )

    val propertyMap = new mutable.HashMap[Id, Set[LogicalProperty]]
    propertyReadOptima foreach {
      case (id, property) =>
        propertyMap(id) = propertyMap.getOrElse(id, Set.empty) + property
    }

    val propertyReadInsertRewriter = bottomUp(Rewriter.lift {
      case lp: LogicalPlan if propertyMap.contains(lp.id) =>
        CacheProperties(lp, propertyMap(lp.id))(attributes.copy(lp.id))
    })

    propertyReadInsertRewriter(logicalPlan).asInstanceOf[LogicalPlan]
  }
//
//  def isAllowedToMoveBelow(projection: Projection, plan: LogicalPlan): Boolean = {
//    plan match {
//      case p if p.lhs.isEmpty || p.rhs.isDefined => false // This guarantees that the last plan for which this returns true has a LHS
//      case _: Expand => true // TODO this is a lie
//      case _: Selection => true
//      case _ => false
//    }
//  }
//
//  def pushdown(projection: Projection, childPlans: Seq[LogicalPlan]): LogicalPlan = {
//    val rewrittenChildPlans = childPlans.toArray
//
//    for (i <- rewrittenChildPlans.indices.reverse) {
//        if (i == rewrittenChildPlans.length - 1) {
//          val source = rewrittenChildPlans(i)
//          val inner = source.lhs.get
//
//          val newProjection = projection.copy(source = inner)(SameId(projection.id)) // TODO assign new cardinality
//
//          // Copy source
//          val newChildrenOfSource = source.children.toSeq.map {
//            case `inner` => newProjection
//            case x => x
//          }
//          rewrittenChildPlans(i) = source.dup(newChildrenOfSource)
//        } else {
//          val source = rewrittenChildPlans(i)
//          val child = rewrittenChildPlans(i + 1)
//
//          // Copy source
//          val newChildrenOfSource = source.children.toSeq.map {
//            case c if c == childPlans(i + 1) => child
//            case x => x
//          }
//          rewrittenChildPlans(i) = source.dup(newChildrenOfSource)
//        }
//    }
//    rewrittenChildPlans.head
//  }
//
//  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
