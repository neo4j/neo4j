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
package org.neo4j.cypher.internal

import java.util.concurrent.atomic.AtomicReference

import org.neo4j.cypher.internal.compatibility.{CompatibilityFor1_9, CompatibilityFor2_2, CompatibilityFor2_3}
import org.neo4j.cypher.{CypherPlanner, CypherRuntime}

import scala.annotation.tailrec
import scala.ref.WeakReference

sealed trait PlannerSpec {
  type SPI
}

case object PlannerSpec_v1_9 extends PlannerSpec {
  type SPI = CompatibilityFor1_9
}

final case class PlannerSpec_v2_2(planner: CypherPlanner) extends PlannerSpec {
  type SPI = CompatibilityFor2_2
}

final case class PlannerSpec_v2_3(planner: CypherPlanner, runtime: CypherRuntime) extends PlannerSpec {
  type SPI = CompatibilityFor2_3
}

trait PlannerFactory {
  def create[S](spec: PlannerSpec { type SPI = S }): S
}

trait PlannerCache[K <: PlannerSpec]  {
  def apply[S](spec: K { type SPI = S }): S
}

object VersionBasedPlannerCache {

  class PlannerMap[K <: PlannerSpec { type SPI = S }, S](factory: PlannerFactory) {
    private val cell = newCell(newRef(Map.empty[K, S]))

    def apply(spec: K): S = recurse(spec, None)

    @tailrec
    private final def recurse(spec: K, candidate: Option[S]): S = {
      val ref = cell.get()
      val map = ref.get.getOrElse(Map.empty)
      map.get(spec) match {
        case Some(result) => result
        case None =>
          val nextCandidate = candidate.getOrElse(factory.create(spec))
          val nextRef = newRef(map.updated(spec, nextCandidate))
          if (cell.compareAndSet(ref, nextRef)) nextCandidate else recurse(spec, candidate orElse Some(nextCandidate))
      }
    }
  }

  private def newCell[T <: AnyRef](v: T) = new AtomicReference(v)
  private def newRef[T <: AnyRef](v: T) = new WeakReference[T](v)
}

class VersionBasedPlannerCache(factory: PlannerFactory) extends PlannerCache[PlannerSpec] {

  import VersionBasedPlannerCache.PlannerMap

  private val cache_v1_9 = new PlannerMap[PlannerSpec_v1_9.type, CompatibilityFor1_9](factory)
  private val cache_v2_2 = new PlannerMap[PlannerSpec_v2_2, CompatibilityFor2_2](factory)
  private val cache_v2_3 = new PlannerMap[PlannerSpec_v2_3, CompatibilityFor2_3](factory)

  def apply[S](spec: PlannerSpec { type SPI = S }): S = spec match {
    case s@PlannerSpec_v1_9 => cache_v1_9(s)
    case s: PlannerSpec_v2_2 => cache_v2_2(s)
    case s: PlannerSpec_v2_3 => cache_v2_3(s)
  }
}

