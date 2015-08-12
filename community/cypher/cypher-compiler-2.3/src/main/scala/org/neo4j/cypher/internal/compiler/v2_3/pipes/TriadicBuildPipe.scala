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

import org.neo4j.collection.primitive.{Primitive, PrimitiveLongObjectMap, PrimitiveLongSet}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v2_3.{CypherTypeException, ExecutionContext}
import org.neo4j.graphdb.Node

case class TriadicBuildPipe(sourcePipe: Pipe, source: String, seen: String)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(sourcePipe, pipeMonitor) with RonjaPipe with NoEffectsPipe {
  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    val triadicState = Primitive.longObjectMap[PrimitiveLongSet]()
    state.triadicState.put(seen, triadicState)
    input.map { ctx =>
      ctx(seen) match {
        case null =>
        case n: Node =>
          ctx(source) match {
            case s: Node =>
              getOrInit(triadicState, s.getId).add(n.getId)
          }
        case x => throw new CypherTypeException(s"Expected a node at `$seen` but got $x")
      }
      ctx
    }.toList.toIterator
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "TriadicBuild", SingleChild(sourcePipe.planDescription),
      Seq(KeyNames(Seq(seen))), identifiers)

  override def withEstimatedCardinality(estimated: Double) =
    copy()(Some(estimated))

  override def symbols =
    sourcePipe.symbols

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(sourcePipe = head)(estimatedCardinality)
  }
}

case class TriadicProbePipe(sourcePipe: Pipe, source: String, seen: String, target: String)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(sourcePipe, pipeMonitor) with RonjaPipe with NoEffectsPipe {

  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    val triadicState = state.triadicState(seen)
    input.filter {
      ctx =>
        ctx(target) match {
          case n: Node => ctx(source) match {
            case s: Node =>
              !triadicState.get(s.getId).contains(n.getId)
            case _ => false
          }
          case _ => false
        }
    }
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "TriadicProbe", SingleChild(sourcePipe.planDescription),
      Seq(KeyNames(Seq(seen, target))), identifiers)

  override def withEstimatedCardinality(estimated: Double) =
    copy()(Some(estimated))

  override def symbols =
    sourcePipe.symbols

  override def dup(sources: List[Pipe]) = {
    val (head :: Nil) = sources
    copy(sourcePipe = head)(estimatedCardinality)
  }
}

object getOrInit {
  def apply(map: PrimitiveLongObjectMap[PrimitiveLongSet], key: Long): PrimitiveLongSet = {
    var result = map.get(key)
    if(result == null) {
      result = Primitive.longSet()
      map.put(key, result)
    }
    result
  }
}
