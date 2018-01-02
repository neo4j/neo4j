/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util

import org.neo4j.cypher.internal.compiler.v2_3.{RuntimeName, PlannerName}
import org.neo4j.cypher.{ExtendedPlanDescription, CypherVersion, PlanDescription}
import org.neo4j.cypher.javacompat.{PlanDescription => JPlanDescription}


class AmendedRootPlanDescription(inner: ExtendedPlanDescription, version: CypherVersion, planner: PlannerName, runtime: RuntimeName)
  extends ExtendedPlanDescription {

  self =>

  val childAsJava = inner.asJava

  def name = inner.name

  def asJava = new JPlanDescription {
    val getName = name

    val getProfilerStatistics = childAsJava.getProfilerStatistics
    val hasProfilerStatistics = childAsJava.hasProfilerStatistics
    val getArguments = {
      val args = childAsJava.getArguments
      val newArgs = new util.HashMap[String, AnyRef]()
      newArgs.putAll(args)
      newArgs.put("version", s"CYPHER ${version.name}")
      newArgs.put("planner", s"Planner ${planner.toTextOutput}")
      newArgs.put("planner-impl", s"Planner ${planner.name}")
      newArgs.put("runtime", s"Runtime ${runtime.toTextOutput}")
      newArgs.put("runtime-impl", s"Runtime ${runtime.name}")
      java.util.Collections.unmodifiableMap[String, AnyRef](newArgs)
    }

    val getChildren = childAsJava.getChildren

    override def toString = self.toString
  }

  override def toString: String = {
    // Have to hack toString here as the alternative would be to release 1.9, 2.0, .. to add a new argument type
    val innerToString = childAsJava.toString
    val arguments = asJava.getArguments
    val version = arguments.get("version")
    val planner = arguments.get("planner")
    val runtime = arguments.get("runtime")
    s"Compiler $version \n\n$planner\n\n$runtime\n\n$innerToString"
  }

  override def children: Seq[PlanDescription] = inner.children

  def extendedChildren: Seq[ExtendedPlanDescription] = inner.extendedChildren

  def arguments: Map[String, AnyRef] = inner.arguments + ("version" -> version.toString)

  def identifiers: Set[String] = inner.identifiers

  override def hasProfilerStatistics: Boolean = inner.hasProfilerStatistics
}
