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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ast.{IdFromSlot, NodeProperty, NullCheck, PrimitiveEquals}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_3.Foldable._
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Equals, Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, Rewriter, bottomUp, topDown}

import scala.collection.mutable

/*
This class takes a logical plan and pipeline information, and rewrites it so it uses register expressions instead of
using Variable. It will also rewrite the pipeline information so that the new plans can be found in there.
 */
class RegisteredRewriter(tokenContext: TokenContext) {

  def apply(in: LogicalPlan, pipelineInformation: Map[LogicalPlan, PipelineInformation]): (LogicalPlan, Map[LogicalPlan, PipelineInformation]) = {
    val newPipelineInfo = mutable.HashMap[LogicalPlan, PipelineInformation]()
    var rewrites = Map[LogicalPlan, LogicalPlan]()
    val rewritePlanWithRegisters = topDown(Rewriter.lift {
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
    resultPlan.findByAllClass[Variable].foreach(v => throw new InternalException(s"Failed to rewrite away $v\n$resultPlan"))

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
      case Property(Variable(key), PropertyKeyName(prop)) =>
        val token: Int = tokenContext.getOptPropertyKeyId(prop).get

        val slot = pipelineInformation(key)
        slot match {
          case LongSlot(offset, false, typ, _) if typ == CTNode => NodeProperty(offset, token)
          case LongSlot(offset, true, typ, _) if typ == CTNode => NullCheck(offset, NodeProperty(offset, token))
        }

      case e@Equals(Variable(k1), Variable(k2)) =>
        val slot1 = pipelineInformation(k1)
        val slot2 = pipelineInformation(k2)
        if (slot1.typ == slot2.typ)
          PrimitiveEquals(IdFromSlot(slot1.offset), IdFromSlot(slot2.offset))
        else
          e
    }
    bottomUp(rewriter = innerRewriter, stopper = stopAtOtherLogicalPlans(thisPlan))
  }

  private def stopAtOtherLogicalPlans(thisPlan: LogicalPlan): (AnyRef) => Boolean = {
    lp => lp.isInstanceOf[LogicalPlan] && lp != thisPlan
  }
}
