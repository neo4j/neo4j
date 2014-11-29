/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.commands.Predicate
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.pipes.expanders.{NodeExpander, NodeRelationshipExpander}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.{LegacyExpression, ExpandExpression}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{ExpandInto, ExpandAll, ExpansionMode}
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.{Direction, Relationship}

final case class ExpandPipe(source: Pipe,
                            from: String,
                            relName: String,
                            to: String,
                            dir: Direction,
                            mode: ExpansionMode,
                            predicate: Option[Predicate] = None,
                            optional: Boolean = false)
                           (nodeExpanderFactory: (Direction, QueryContext) => NodeExpander[Relationship])
                           (val estimatedCardinality: Option[Long] = None)
                           (implicit pipeMonitor: PipeMonitor)
           extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    ExpandPipeGenerator(input, from, relName, to, dir, mode, predicate, optional)(nodeExpanderFactory)(state).iterator

  override def localEffects = Effects.READS_ENTITIES

  val symbols = source.symbols.add(to, CTNode).add(relName, CTRelationship)

  def planDescription = {
    val nameStr = if (optional) "OptionalExpand" else "Expand"
    val modeStr = if (mode == ExpandInto) "Into" else ""
    val name = s"$nameStr$modeStr"

    predicate match {
      case None =>
        source.planDescription.andThen(this, name, identifiers, ExpandExpression(from, relName, to, dir))
      case Some(expression) =>
        source.planDescription.andThen(this, name, identifiers, ExpandExpression(from, relName, to, dir), LegacyExpression(expression))
    }
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(nodeExpanderFactory)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Long) = copy()(nodeExpanderFactory)(Some(estimated))
}

// TODO: Remove uses
object ExpandPipeForStringTypes {
  def apply(source: Pipe,
            from: String,
            relName: String,
            to: String,
            dir: Direction,
            types: Seq[String] = Seq.empty,
            mode: ExpansionMode = ExpandAll)
           (estimatedCardinality: Option[Long] = None)
           (implicit pipeMonitor: PipeMonitor) = {
    ExpandPipe(source, from, relName, to, dir, mode)(NodeRelationshipExpander.forTypeNames(types: _*))(estimatedCardinality)
  }
}

// TODO: Remove uses
object ExpandPipeForIntTypes {
  def apply(source: Pipe,
            from: String,
            relName: String,
            to: String,
            dir: Direction,
            types: Seq[Int] = Seq.empty,
            mode: ExpansionMode = ExpandAll)
           (estimatedCardinality: Option[Long] = None)
           (implicit pipeMonitor: PipeMonitor) = {
    ExpandPipe(source, from, relName, to, dir, mode)(NodeRelationshipExpander.forTypeIds(types: _*))(estimatedCardinality)
  }
}
