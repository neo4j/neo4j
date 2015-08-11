/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.collection.primitive.Primitive
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v2_3.{CypherTypeException, ExecutionContext}
import org.neo4j.graphdb.Node

case class TriadicBuildPipe(source: Pipe, identifier: String)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe with NoEffectsPipe {
  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    val triadicSeen = Primitive.longSet()
    state.triadicSets.put(identifier, triadicSeen)
    input.map { ctx =>
      ctx(identifier) match {
        case null =>
        case n: Node =>
          triadicSeen.add(n.getId)
        case x => throw new CypherTypeException(s"Expected a node at `$identifier` but got $x")
      }
      ctx
    }.toList.toIterator
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "TriadicBuild", SingleChild(source.planDescription),
      Seq(KeyNames(Seq(identifier))), identifiers)

  override def withEstimatedCardinality(estimated: Double) =
    copy()(Some(estimated))

  override def symbols =
    source.symbols

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }
}

case class TriadicProbePipe(source: Pipe, triadicSet: String, identifier: String)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe with NoEffectsPipe {

  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    val set = state.triadicSets(triadicSet)
    input.filter {
      ctx =>
        ctx(identifier) match {
          case n: Node => !set.contains(n.getId)
          case _ => false
        }
    }
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "TriadicProbe", SingleChild(source.planDescription),
      Seq(KeyNames(Seq(triadicSet, identifier))), identifiers)

  override def withEstimatedCardinality(estimated: Double) =
    copy()(Some(estimated))

  override def symbols =
    source.symbols

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }
}
