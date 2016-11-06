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
package org.neo4j.cypher.internal.compatibility.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.neo4j.cypher.internal.compiler.v3_2.{PlannerName, RuntimeName}
import org.neo4j.cypher.internal.javacompat.{PlanDescription, ProfilerStatistics}
import org.neo4j.cypher.{CypherVersion, InternalException}

import scala.collection.JavaConverters._

case class CompatibilityPlanDescription(inner: InternalPlanDescription, version: CypherVersion,
                                        planner: PlannerName, runtime: RuntimeName)
  extends org.neo4j.cypher.internal.PlanDescription {

  self =>

  def children = exceptionHandler.runSafely {
    inner.children.toIndexedSeq.map(CompatibilityPlanDescription.apply(_, version, planner, runtime))
  }

  def arguments: Map[String, AnyRef] = exceptionHandler.runSafely {
    inner.arguments.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap
  }

  def identifiers = exceptionHandler.runSafely {
    inner.orderedVariables.toSet
  }

  override def hasProfilerStatistics = exceptionHandler.runSafely {
    inner.arguments.exists(_.isInstanceOf[DbHits])
  }

  def name = exceptionHandler.runSafely {
    inner.name
  }

  def asJava: PlanDescription = exceptionHandler.runSafely {
    asJava(self)
  }

  override def toString: String = {
    val NL = System.lineSeparator()
    exceptionHandler.runSafely {
      s"Compiler CYPHER ${version.name}$NL${NL}Planner ${planner.toTextOutput.toUpperCase}$NL${NL}Runtime ${runtime.toTextOutput.toUpperCase}$NL$NL$inner"
    }
  }

  def asJava(in: org.neo4j.cypher.internal.PlanDescription): PlanDescription = new PlanDescription {
    def getProfilerStatistics: ProfilerStatistics = new ProfilerStatistics {
      def getDbHits: Long = extract { case DbHits(count) => count }

      def getRows: Long = extract { case Rows(count) => count }

      private def extract(f: PartialFunction[Argument, Long]): Long =
        inner.arguments.collectFirst(f).getOrElse(throw new InternalException("Don't have profiler stats"))
    }

    def getName: String = name

    def hasProfilerStatistics: Boolean = self.hasProfilerStatistics

    def getArguments: java.util.Map[String, AnyRef] = arguments.asJava

    def getIdentifiers: java.util.Set[String] = identifiers.asJava

    def getChildren: java.util.List[PlanDescription] = in.children.toList.map(_.asJava).asJava

    override def toString: String = self.toString
  }
}
