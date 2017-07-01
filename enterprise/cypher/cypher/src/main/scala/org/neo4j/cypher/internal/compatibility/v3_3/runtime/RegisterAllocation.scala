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

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, ast}
import org.neo4j.cypher.internal.ir.v3_3.IdName

import scala.collection.mutable

object RegisterAllocation {
  def allocateRegisters(lp: LogicalPlan): Map[LogicalPlan, PipelineInformation] = {

    val result = new mutable.OpenHashMap[LogicalPlan, PipelineInformation]()

    def allocate(lp: LogicalPlan, nullable: Boolean, argument: Option[PipelineInformation]): PipelineInformation = lp match {
      case Aggregation(source, groupingExpressions, aggregationExpressions) =>
        val oldPipeline = allocate(source, nullable, argument)
        val newPipeline = PipelineInformation.empty

        def addExpressions(groupingExpressions: Map[String, Expression]) = {
          groupingExpressions foreach {
            case (key, ast.Variable(ident)) =>
              val slotInfo = oldPipeline(ident)
              newPipeline.add(ident, slotInfo)
            case (key, exp) =>
              newPipeline.newReference(key, nullable = true, CTAny)
          }
        }

        addExpressions(groupingExpressions)
        addExpressions(aggregationExpressions)
        result += (lp -> newPipeline)
        newPipeline

      case Projection(source, expressions) =>
        val pipeline = allocate(source, nullable, argument)
        expressions foreach {
          case (key, ast.Variable(ident)) =>
            // it's already there. no need to add a new slot for it
          case (key, exp) =>
            pipeline.newReference(key, nullable = true, CTAny)
        }
        result += (lp -> pipeline)
        pipeline

      case leaf: NodeLogicalLeafPlan =>
        val pipeline = argument.getOrElse(PipelineInformation.empty)
        pipeline.newLong(leaf.idName.name, nullable, CTNode)
        result += (lp -> pipeline)
        pipeline

      case ProduceResult(_, source) =>
        val pipeline = allocate(source, nullable, argument)
        result += (lp -> pipeline)
        pipeline

      case Selection(_, source) =>
        val pipeline = allocate(source, nullable, argument)
        result += (lp -> pipeline)
        pipeline

      case Expand(source, _, _, _, IdName(to), IdName(relName), ExpandAll) =>
        val oldPipeline = allocate(source, nullable, argument)
        val newPipeline = oldPipeline.deepClone()
        newPipeline.newLong(relName, nullable, CTRelationship)
        newPipeline.newLong(to, nullable, CTNode)
        result += (lp -> newPipeline)
        newPipeline

      case Expand(source, _, _, _, _, IdName(relName), ExpandInto) =>
        val oldPipeline = allocate(source, nullable, argument)
        val newPipeline = oldPipeline.deepClone()
        newPipeline.newLong(relName, nullable, CTRelationship)
        result += (lp -> newPipeline)
        newPipeline

      case Optional(source, _) =>
        val pipeline = allocate(source, nullable = true, argument)
        result += (lp -> pipeline)
        pipeline

      case OptionalExpand(source, IdName(from), _,_, IdName(to), IdName(rel), ExpandAll, _) =>
        val oldPipeline = allocate(source, nullable, argument)
        val newPipeline = oldPipeline.deepClone()
        newPipeline.newLong(rel, nullable = true, CTRelationship)
        newPipeline.newLong(to, nullable = true, CTNode)
        result += (lp -> newPipeline)
        newPipeline

      case OptionalExpand(source, IdName(from), _,_, IdName(to), IdName(rel), ExpandInto, _) =>
        val oldPipeline = allocate(source, nullable, argument)
        val newPipeline = oldPipeline.deepClone()
        newPipeline.newLong(rel, nullable = true, CTRelationship)
        result += (lp -> newPipeline)
        newPipeline

      case Skip(source, _) =>
        val pipeline = allocate(source, nullable, argument)
        result += (lp -> pipeline)
        pipeline

      case Apply(lhs, rhs) =>
        val lhsPipeline = allocate(lhs, nullable, argument)
        val rhsPipeline = allocate(rhs, nullable, Some(lhsPipeline.deepClone()))
        result += (lp -> rhsPipeline)
        rhsPipeline

      case CartesianProduct(lhs, rhs) =>
        val lhsPipeline = allocate(lhs, nullable, argument)
        val rhsPipeline = allocate(rhs, nullable, argument)
        val cartesianProductPipeline = lhsPipeline.deepClone()
        rhsPipeline.foreachSlot {
          case (k,slot) =>
            cartesianProductPipeline.add(k, slot)
        }
        result += (lp -> cartesianProductPipeline)
        cartesianProductPipeline


      case p => throw new RegisterAllocationFailed(s"Don't know how to handle $p")
    }

    allocate(lp, nullable = false, None)

    result.toMap
  }
}

class RegisterAllocationFailed(str: String) extends InternalException(str)