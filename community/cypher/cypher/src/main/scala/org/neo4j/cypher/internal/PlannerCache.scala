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

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.compatibility.{CompatibilityFor1_9, CompatibilityFor2_2, CompatibilityFor2_3}
import org.neo4j.cypher.{CypherPlanner, CypherRuntime}

import scala.collection.JavaConverters._
import scala.collection.concurrent.Map

class PlannerCache(factory: PlannerFactory) {
  private val map: Map[Any, Any] = new ConcurrentHashMap[Any, Any]().asScala

  def apply[T](spec: PlannerSpec[T]): T = {
    map.get(spec) match {
      case Some(cachedInstance) => cachedInstance.asInstanceOf[T]
      case None =>
        val newInstance = factory.create(spec)
        map.putIfAbsent(spec, newInstance) match {
          case Some(cachedInstance) => cachedInstance.asInstanceOf[T]
          case None => newInstance
        }
    }
  }

}

sealed trait PlannerSpec[T]

case object PlannerSpec_v1_9 extends PlannerSpec[CompatibilityFor1_9]
case class PlannerSpec_v2_2(planner: CypherPlanner) extends PlannerSpec[CompatibilityFor2_2]
case class PlannerSpec_v2_3(planner: CypherPlanner, runtime: CypherRuntime) extends PlannerSpec[CompatibilityFor2_3]

trait PlannerFactory {

  def create[T](spec: PlannerSpec[T]): T
}
