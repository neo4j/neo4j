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

import org.neo4j.collection.primitive.PrimitiveLongSet
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_3.{CypherTypeException, ExecutionContext}
import org.neo4j.graphdb.Node

case class TriadicPipe(left: Pipe, source: String, seen: String, target: String, right: Pipe)
                      (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(left, pipeMonitor) with RonjaPipe with NoEffectsPipe {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    var triadicState: PrimitiveLongSet = null
    // 1. Build
    new LazyGroupingIterator[ExecutionContext](input) {
      override def getKey(row: ExecutionContext) = row(source)

      override def getValue(row: ExecutionContext) = row(seen) match {
        case n: Node => Some(n.getId)
        case null => None
        case x => throw new CypherTypeException(s"Expected a node at `$seen` but got $x")
      }

      override def setState(triadicSet: PrimitiveLongSet) = triadicState = triadicSet

    // 2. pass through 'right'
    }.flatMap { (outerContext) =>
      val original = outerContext.clone()
      val innerState = state.withInitialContext(outerContext)
      val innerResults = right.createResults(innerState)
      innerResults.map { context => context ++ original }

    // 3. Probe
    }.filter { ctx =>
      ctx(target) match {
        case n: Node => !triadicState.contains(n.getId)
        case _ => false
      }
    }
  }

  override def planDescriptionWithoutCardinality = new PlanDescriptionImpl(
    id = id,
    name = "Triadic",
    children = TwoChildren(left.planDescription, right.planDescription),
    arguments = Seq(KeyNames(Seq(source, seen, target))),
    identifiers = identifiers)

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def symbols: SymbolTable = left.symbols.add(right.symbols.identifiers)

  override def dup(sources: List[Pipe]) = {
    val (left :: right :: Nil) = sources
    copy(left = left, right = right)(estimatedCardinality)
  }
}
