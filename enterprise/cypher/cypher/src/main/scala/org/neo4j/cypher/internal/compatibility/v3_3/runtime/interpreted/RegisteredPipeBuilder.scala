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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commandExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes.{AllNodesScanRegisterPipe, ExpandAllRegisterPipe, NodeIndexSeekRegisterPipe, ProduceResultRegisterPipe, _}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.{expressions => runtimeExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.phases.Monitors
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticTable, ast => frontEndAst}
import org.neo4j.cypher.internal.ir.v3_3.IdName

class RegisteredPipeBuilder(fallback: PipeBuilder,
                            expressionConverter: ExpressionConverters,
                            idMap: Map[LogicalPlan, Id],
                            monitors: Monitors,
                            pipelines: Map[LogicalPlan, PipelineInformation],
                            readOnly : Boolean,
                            buildExpression: (frontEndAst.Expression) => frontEndAst.Expression)
                           (implicit context: PipeExecutionBuilderContext, planContext: PlanContext) extends PipeBuilder {

  private val convertExpressions = buildExpression andThen expressionConverter.toCommandExpression

  override def build(plan: LogicalPlan): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = idMap.getOrElse(plan, new Id)
    val pipelineInformation = pipelines(plan)

    plan match {
      case AllNodesScan(IdName(column), _) =>
        AllNodesScanRegisterPipe(column, pipelineInformation)(id)

      case NodeIndexSeek(IdName(column),label,propertyKeys,valueExpr, _) =>
        val indexSeekMode = IndexSeekModeFactory(unique = false, readOnly = readOnly).fromQueryExpression(valueExpr)
        NodeIndexSeekRegisterPipe(column, label, propertyKeys,
          valueExpr.map(convertExpressions), indexSeekMode,pipelineInformation)(id = id)

      case Argument(_) =>
        ArgumentRegisterPipe(pipelineInformation)(id)

      case NodeByLabelScan(IdName(column), label, _) =>
        NodesByLabelScanRegisterPipe(column, LazyLabel(label), pipelineInformation)(id)

      case _ => fallback.build(plan)
    }
  }

  override def build(plan: LogicalPlan, source: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = idMap.getOrElse(plan, new Id)
    val pipeline = pipelines(plan)

    plan match {
      case ProduceResult(columns, _) =>
        val runtimeColumns = createProjectionsForResult(columns, pipeline)
        ProduceResultRegisterPipe(source, runtimeColumns)(id)

      case Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandAll) =>
        val fromSlot = pipeline(from)
        val relSlot = pipeline(relName)
        val toSlot = pipeline(to)
        ExpandAllRegisterPipe(source, fromSlot.offset, relSlot.offset, toSlot.offset, dir, LazyTypes(types), pipeline)(id)

      case Expand(_, IdName(from), dir, types, IdName(to), IdName(relName), ExpandInto) =>
        val fromSlot = pipeline(from)
        val relSlot = pipeline(relName)
        val toSlot = pipeline(to)
        ExpandIntoRegisterPipe(source, fromSlot.offset, relSlot.offset, toSlot.offset, dir, LazyTypes(types), pipeline)(id)

      case Optional(inner, symbols) =>
        val nullableKeys = inner.availableSymbols -- symbols
        val nullableOffsets = nullableKeys.map(k => pipeline.getLongOffsetFor(k.name))
        OptionalRegisteredPipe(source, nullableOffsets.toSeq, pipeline)(id)

      case _ => fallback.build(plan, source)
    }
  }

  private def createProjectionsForResult(columns: Seq[String], pipelineInformation1: PipelineInformation) = {
    val runtimeColumns: Seq[(String, commandExpressions.Expression)] = columns map {
      k =>
        pipelineInformation1(k) match {
          case LongSlot(offset, false, CTNode, _) =>
            k -> runtimeExpressions.NodeFromRegister(offset)
          case LongSlot(offset, true, CTNode, _) =>
            k -> runtimeExpressions.NullCheck(offset, runtimeExpressions.NodeFromRegister(offset))
          case _ =>
            throw new InternalException("Did not find " + k + "in the pipeline information1")
        }
    }
    runtimeColumns
  }

  override def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe): Pipe = {
    implicit val table: SemanticTable = context.semanticTable

    val id = idMap.getOrElse(plan, new Id)

    plan match {
      case Apply(_,_) => ApplyRegisterPipe(lhs, rhs)(id)
      case _ => fallback.build(plan, lhs, rhs)
    }
  }
}
