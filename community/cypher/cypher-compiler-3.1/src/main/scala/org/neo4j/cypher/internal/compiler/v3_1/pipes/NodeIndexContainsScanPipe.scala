/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, ReadsGivenNodeProperty, ReadsNodesWithLabels}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.{Index, LegacyExpression}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_1.ast.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CTNode
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.index.IndexDescriptor

abstract class AbstractNodeIndexStringScanPipe(ident: String,
                                               label: LabelToken,
                                               propertyKey: PropertyKeyToken,
                                               valueExpr: Expression)(implicit pipeMonitor: PipeMonitor)
  extends Pipe with RonjaPipe {

  private val descriptor = new IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val value = valueExpr(baseContext)(state)

    val resultNodes = value match {
      case value: String =>
        queryContextCall(state, descriptor, value).
          map(node => baseContext.newWith1(ident, node))
      case null =>
        Iterator.empty
      case x => throw new CypherTypeException(s"Expected a string value, but got $x")
    }

    resultNodes
  }

  protected def queryContextCall(state: QueryState, indexDescriptor: IndexDescriptor, value: String): Iterator[Node]

  override def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  override def symbols = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  override def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  override def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsNodesWithLabels(label.name), ReadsGivenNodeProperty(propertyKey.name))
}

case class NodeIndexContainsScanPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     valueExpr: Expression)
                                    (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends AbstractNodeIndexStringScanPipe(ident, label, propertyKey, valueExpr) {

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def planDescriptionWithoutCardinality = {
    val arguments = Seq(Index(label.name, propertyKey.name), LegacyExpression(valueExpr))
    new PlanDescriptionImpl(this.id, "NodeIndexContainsScan", NoChildren, arguments, variables)
  }

  override protected def queryContextCall(state: QueryState, indexDescriptor: IndexDescriptor, value: String) =
    state.query.indexScanByContains(indexDescriptor, value)
}

case class NodeIndexEndsWithScanPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     valueExpr: Expression)
                                    (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends AbstractNodeIndexStringScanPipe(ident, label, propertyKey, valueExpr) {

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def planDescriptionWithoutCardinality = {
    val arguments = Seq(Index(label.name, propertyKey.name), LegacyExpression(valueExpr))
    new PlanDescriptionImpl(this.id, "NodeIndexEndsWithScan", NoChildren, arguments, variables)
  }

  override protected def queryContextCall(state: QueryState, indexDescriptor: IndexDescriptor, value: String) =
    state.query.indexScanByEndsWith(indexDescriptor, value)
}