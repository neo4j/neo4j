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
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution.convert

import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.{PathBuilder, ProjectedPath}
import ProjectedPath.{Step => RuntimeStep}
import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.helpers.ThisShouldNotHappenError

object ProjectedPathConverter {

  sealed abstract class Step {
    def dependencies: Set[IdName]
  }

  final case class SingleNodeStep(node: IdName) extends Step {
    val dependencies = Set(node)
  }

  final case class VarLengthRelationshipStep(relName: IdName, dependencies: Set[IdName]) extends Step

  final case class SingleRelationshipStep(relName: IdName, dependencies: Set[IdName]) extends Step

  implicit class NamedPathConverter(val namedPath: NamedPath) {
    def asSteps: Seq[Step] = namedPath match {
      case NamedNodePath(_, node) =>
        Seq(SingleNodeStep(node))

      case NamedRelPath(_, patternRels) if patternRels.length == 0 =>
        throw new ThisShouldNotHappenError("Davide", "Encountered NamedRelPath without PatternRelationships")

      case NamedRelPath(_, patternRels) =>
        val steps = Seq.newBuilder[Step]

        steps += SingleNodeStep(patternRels.head.left)

        for ( rel <- patternRels ) {
          rel.length match {
            case SimplePatternLength =>
              steps += SingleRelationshipStep(rel.name, rel.coveredIds)

            case _: VarPatternLength =>
              steps += VarLengthRelationshipStep(rel.name, rel.coveredIds)
          }
        }

        steps.result()
    }

    def asProjectedPath = asSteps.asProjectedPath
  }

  implicit class StepConverter(val steps: Seq[Step]) {
    def asRuntimeSteps: Seq[RuntimeStep] = steps.map {

      case SingleNodeStep(IdName(node)) =>
        (ctx: ExecutionContext, builder: PathBuilder) => builder += ctx(node).asInstanceOf[Node]; ()

      case SingleRelationshipStep(IdName(rel), _) =>
        (ctx: ExecutionContext, builder: PathBuilder) => builder += ctx(rel).asInstanceOf[Relationship]; ()

      case VarLengthRelationshipStep(IdName(rel), _) =>
        (ctx: ExecutionContext, builder: PathBuilder) =>
          val iterator = ctx(rel).asInstanceOf[Iterable[Relationship]].iterator
          while (iterator.hasNext)
            builder += iterator.next()
    }

    def asDependencies = steps.flatMap(_.dependencies).map(_.name).toSet
    def asProjectedPath = ProjectedPath(asDependencies, asRuntimeSteps: _*)
  }
}

