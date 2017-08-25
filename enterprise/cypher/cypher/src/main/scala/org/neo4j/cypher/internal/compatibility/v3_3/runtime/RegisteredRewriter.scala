/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ast._
import org.neo4j.cypher.internal.compiler.v3_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v3_3.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_3.Foldable._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, Rewriter, topDown, ast => frontendAst}

import scala.collection.mutable

/*
This class takes a logical plan and pipeline information, and rewrites it so it uses register expressions instead of
using Variable. It will also rewrite the pipeline information so that the new plans can be found in there.
 */
class RegisteredRewriter(tokenContext: TokenContext) {

  private def rewriteUsingIncoming(oldPlan: LogicalPlan): Boolean = oldPlan match {
    case _: Aggregation | _: Distinct => true
    case _ => false
  }

  def apply(in: LogicalPlan, pipelineInformation: Map[LogicalPlan, PipelineInformation]): (LogicalPlan, Map[LogicalPlan, PipelineInformation]) = {
    val newPipelineInfo = mutable.HashMap[LogicalPlan, PipelineInformation]()
    var rewrites = Map[LogicalPlan, LogicalPlan]()
    val rewritePlanWithRegisters = topDown(Rewriter.lift {
      /*
      Projection means executing expressions and writing the result to a row. Since any expression of Variable-type
      would just write to the row the data that is already in it, we can just skip them
       */
      case oldPlan@Projection(_, expressions) =>
        val information = pipelineInformation(oldPlan)
        val rewriter = rewriteCreator(information, oldPlan)

        val newExpressions = expressions collect {
          case (column, expression) => column -> expression.endoRewrite(rewriter)
        }

        val newPlan = oldPlan.copy(expressions = newExpressions)(oldPlan.solved)
        newPipelineInfo += (newPlan -> information)

        rewrites += (oldPlan -> newPlan)

        newPlan

      case oldPlan: VarExpand =>
        /*
        The node and edge predicates will be set and evaluated on the incoming rows, not on the outgoing ones.
        We need to use the incoming pipeline info for predicate rewriting
         */
        val incomingPipeline = pipelineInformation(oldPlan.left)
        val rewriter = rewriteCreator(incomingPipeline, oldPlan)

        val newNodePredicate = oldPlan.nodePredicate.endoRewrite(rewriter)
        val newEdgePredicate = oldPlan.edgePredicate.endoRewrite(rewriter)

        val newPlan = oldPlan.copy(
          nodePredicate = newNodePredicate,
          edgePredicate = newEdgePredicate,
          legacyPredicates = Seq.empty // If we use the legacy predicates, we are not on the register runtime
        )(oldPlan.solved)

        /*
        Since the logical plan pipeinformation is about the output rows we still need to remember the
        outgoing pipeline info here
         */
        val outgoingPipeline = pipelineInformation(oldPlan)
        newPipelineInfo += (newPlan -> outgoingPipeline)

        rewrites += (oldPlan -> newPlan)

        newPlan
        
      case oldPlan: LogicalPlan if rewriteUsingIncoming(oldPlan) =>
        val incomingPipeline = pipelineInformation(oldPlan.lhs.getOrElse(throw new InternalException("Leaf plans cannot be rewritten this way")))
        val rewriter = rewriteCreator(incomingPipeline, oldPlan)
        val newPlan = oldPlan.endoRewrite(rewriter)

        /*
        Since the logical plan pipeinformation is about the output rows we still need to remember the
        outgoing pipeline info here
         */
        val outgoingPipeline = pipelineInformation(oldPlan)
        newPipelineInfo += (newPlan -> outgoingPipeline)
        rewrites += (oldPlan -> newPlan)
        newPlan

      case oldPlan: LogicalPlan =>
        val information = pipelineInformation(oldPlan)
        val rewriter = rewriteCreator(information, oldPlan)
        val newPlan = oldPlan.endoRewrite(rewriter)
        newPipelineInfo += (newPlan -> information)

        rewrites += (oldPlan -> newPlan)

        newPlan
    })

    // Rewrite plan and note which logical plans are rewritten to something else
    val resultPlan = in.endoRewrite(rewritePlanWithRegisters)

    // TODO: This should probably only run when -ea is enabled
    resultPlan.findByAllClass[Variable].foreach(v => throw new CantCompileQueryException(s"Failed to rewrite away $v\n$resultPlan"))

    // re-apply the rewrites to the keys of the pipeline information
    val rewriter = createRewriterFrom(rewrites)
    val massagesPipelineinfo = newPipelineInfo.toMap map {
      case (plan: LogicalPlan, v: PipelineInformation) =>
        plan.endoRewrite(rewriter) -> v
    }

    (resultPlan, massagesPipelineinfo)
  }

  private def createRewriterFrom(rewrites: Map[LogicalPlan, LogicalPlan]) = topDown(Rewriter.lift {
    case x: LogicalPlan if rewrites.contains(x) =>
      rewrites(x)
  })

  private def rewriteCreator(pipelineInformation: PipelineInformation, thisPlan: LogicalPlan): Rewriter = {
    val innerRewriter = Rewriter.lift {
      case Property(Variable(key), PropertyKeyName(propKey)) =>
        val maybeToken: Option[Int] = tokenContext.getOptPropertyKeyId(propKey)

        val slot = pipelineInformation(key)
        val propExpression = (slot, maybeToken) match {
          case (LongSlot(offset, _, typ, name), Some(token)) if typ == CTNode => NodeProperty(offset, token, s"$name.$propKey")
          case (LongSlot(offset, _, typ, name), None) if typ == CTNode => NodePropertyLate(offset, propKey, s"$name.$propKey")
          case (LongSlot(offset, _, typ, name), Some(token)) if typ == CTRelationship => RelationshipProperty(offset, token, s"$name.$propKey")
          case (LongSlot(offset, _, typ, name), None) if typ == CTRelationship => RelationshipPropertyLate(offset, propKey, s"$name.$propKey")
          case _ => throw new CantCompileQueryException(s"Expressions on object other then nodes and relationships are not yet supported")
        }

        if (slot.nullable)
          NullCheck(slot.offset, propExpression)
        else
          propExpression

      case e@Equals(Variable(k1), Variable(k2)) => // TODO: Handle nullability
        val slot1 = pipelineInformation(k1)
        val slot2 = pipelineInformation(k2)
        if (slot1.typ == slot2.typ)
          PrimitiveEquals(IdFromSlot(slot1.offset), IdFromSlot(slot2.offset))
        else
          e

      case GetDegree(Variable(n), typ, direction) =>
        val maybeToken: Option[String] = typ.map(r => r.name)
        pipelineInformation(n) match {
          case LongSlot(offset, false, CTNode, _) => GetDegreePrimitive(offset, maybeToken, direction)
          case LongSlot(offset, true, CTNode, _) => NullCheck(offset, GetDegreePrimitive(offset, maybeToken, direction))
          case _ => throw new CantCompileQueryException(s"Invalid slot for GetDegree: $n")
        }

      case Variable(k) =>
        pipelineInformation.get(k) match {
          case Some(slot) => slot match {
            case LongSlot(offset, false, CTNode, name) => NodeFromRegister(offset, name)
            case LongSlot(offset, true, CTNode, name) => NullCheck(offset, NodeFromRegister(offset, name))
            case LongSlot(offset, false, CTRelationship, name) => RelationshipFromRegister(offset, name)
            case LongSlot(offset, true, CTRelationship, name) => NullCheck(offset, RelationshipFromRegister(offset, name))
            case RefSlot(offset, _, _, _) => ReferenceFromRegister(offset)
            case _ =>
              throw new CantCompileQueryException("Unknown type for `" + k + "` in the pipeline information")
          }
          case _ =>
            throw new CantCompileQueryException("Did not find `" + k + "` in the pipeline information")
        }

      case idFunction: FunctionInvocation if idFunction.function == frontendAst.functions.Id =>
        idFunction.args.head match {
          case Variable(key) =>
            val slot = pipelineInformation(key)
            slot match {
              case LongSlot(offset, true, _, _) => NullCheck(offset, IdFromSlot(offset))
              case LongSlot(offset, false, _, _) => IdFromSlot(offset)
              case _ => idFunction // Don't know how to specialize this
            }
          case e => idFunction // Don't know how to specialize this
        }

      case idFunction: FunctionInvocation if idFunction.function == frontendAst.functions.Exists =>
        idFunction.args.head match {
          case Property(Variable(key), PropertyKeyName(propKey)) =>
            checkIfPropertyExists(pipelineInformation, key, propKey)
          case e => idFunction // Don't know how to specialize this
        }

      case e@IsNull(Property(Variable(key), PropertyKeyName(propKey))) =>
        Not(checkIfPropertyExists(pipelineInformation, key, propKey))(e.position)

      case _: ShortestPathExpression =>
        throw new CantCompileQueryException(s"Expressions with shortestPath functions not yet supported in register allocation")

      case _: ScopeExpression | _: NestedPlanExpression =>
        throw new CantCompileQueryException(s"Expressions with inner scope are not yet supported in register allocation")

      case _: PatternExpression =>
        throw new CantCompileQueryException(s"Pattern expressions not yet supported in the slotted runtime")
    }
    topDown(rewriter = innerRewriter, stopper = stopAtOtherLogicalPlans(thisPlan))
  }

  private def checkIfPropertyExists(pipelineInformation: PipelineInformation, key: String, propKey: String) = {
    val slot = pipelineInformation(key)
    val maybeToken = tokenContext.getOptPropertyKeyId(propKey)

    val propExpression = (slot, maybeToken) match {
      case (LongSlot(offset, _, typ, name), Some(token)) if typ == CTNode =>
        NodePropertyExists(offset, token, s"$name.$propKey")

      case (LongSlot(offset, _, typ, name), None) if typ == CTNode =>
        NodePropertyExistsLate(offset, propKey, s"$name.$propKey")

      case (LongSlot(offset, _, typ, name), Some(token)) if typ == CTRelationship =>
        RelationshipPropertyExists(offset, token, s"$name.$propKey")

      case (LongSlot(offset, _, typ, name), None) if typ == CTRelationship =>
        RelationshipPropertyExistsLate(offset, propKey, s"$name.$propKey")

      case _ => throw new CantCompileQueryException(s"Expressions on object other then nodes and relationships are not yet supported")
    }

    if (slot.nullable)
      NullCheck(slot.offset, propExpression)
    else
      propExpression
  }

  private def stopAtOtherLogicalPlans(thisPlan: LogicalPlan): (AnyRef) => Boolean = {
    lp => lp.isInstanceOf[LogicalPlan] && lp != thisPlan
  }
}
