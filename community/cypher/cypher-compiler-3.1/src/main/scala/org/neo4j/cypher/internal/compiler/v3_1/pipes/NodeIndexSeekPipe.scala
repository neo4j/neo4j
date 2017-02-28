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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{Expression, InequalitySeekRangeExpression, PrefixSeekRangeExpression}
import org.neo4j.cypher.internal.compiler.v3_1.commands.{QueryExpression, RangeQueryExpression, indexQuery}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, ReadsGivenNodeProperty, ReadsNodesWithLabels}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.{Index, InequalityIndex, PrefixIndex}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v3_1.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.InternalException
import org.neo4j.cypher.internal.frontend.v3_1.ast.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CTNode

case class NodeIndexSeekPipe(ident: String,
                             label: LabelToken,
                             propertyKey: PropertyKeyToken,
                             valueExpr: QueryExpression[Expression],
                             indexMode: IndexSeekMode = IndexSeek)
                            (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends Pipe with RonjaPipe {

  valueExpr.expression.registerOwningPipe(this)

  private val descriptor = IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  private val indexFactory = indexMode.indexFactory(descriptor)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val index = indexFactory(state)
    val baseContext = state.initialContext.getOrElse(ExecutionContext.empty)
    val resultNodes = indexQuery(valueExpr, baseContext, state, index, label.name, propertyKey.name)
    resultNodes.map(node => baseContext.newWith1(ident, node))
  }

  def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  def planDescriptionWithoutCardinality = {
    val name = indexMode.name
    val indexDesc = indexMode match {
      case IndexSeekByRange | UniqueIndexSeekByRange =>
        valueExpr match {
          case RangeQueryExpression(PrefixSeekRangeExpression(PrefixRange(prefix))) =>
            PrefixIndex(label.name, propertyKey.name, prefix)

          case RangeQueryExpression(InequalitySeekRangeExpression(RangeLessThan(bounds))) =>
            InequalityIndex(label.name, propertyKey.name, bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq)

          case RangeQueryExpression(InequalitySeekRangeExpression(RangeGreaterThan(bounds))) =>
            InequalityIndex(label.name, propertyKey.name, bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq)

          case RangeQueryExpression(InequalitySeekRangeExpression(RangeBetween(greaterThanBounds, lessThanBounds))) =>
            val greaterThanBoundsText = greaterThanBounds.bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq
            val lessThanBoundsText = lessThanBounds.bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint}").toIndexedSeq
            InequalityIndex(label.name, propertyKey.name, greaterThanBoundsText ++ lessThanBoundsText)

          case _ =>
            throw new InternalException("This should never happen. Missing a case?")
        }
      case IndexSeek | LockingUniqueIndexSeek | UniqueIndexSeek => Index(label.name, propertyKey.name)
      case _ => throw new InternalException("This should never happen. Missing a case?")
    }
    new PlanDescriptionImpl(this.id, name, NoChildren, Seq(indexDesc), variables)
  }

  def symbols = new SymbolTable(Map(ident -> CTNode))

  override def monitor = pipeMonitor

  def dup(sources: List[Pipe]): Pipe = {
    require(sources.isEmpty)
    this
  }

  def sources: Seq[Pipe] = Seq.empty

  override def localEffects = Effects(ReadsNodesWithLabels(label.name), ReadsGivenNodeProperty(propertyKey.name))

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
