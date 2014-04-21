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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.ProjectedPath
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.ProjectedPath.{Projector => PathProjector}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb.Direction

object namedPathConverter extends (NamedPath => ProjectedPath) {

  def apply(namedPath: NamedPath): ProjectedPath = {
    val pathDependencies = namedPath.dependencies.map(_.name)
    val pathProjector = projector(namedPath)
    ProjectedPath(pathDependencies, pathProjector)
  }

  def projector(namedPath: NamedPath): PathProjector = projector(namedPath, ProjectedPath.nilProjector)

  def projector(namedPath: NamedPath, tail: PathProjector): PathProjector = namedPath match {

    case NamedNodePath(_, node) =>
      ProjectedPath.singleNodeProjector(node.name, tail)

    case NamedRelPath(_, patternRels) if patternRels.length == 0 =>
      throw new ThisShouldNotHappenError("Davide", "Encountered NamedRelPath without PatternRelationships")

    case NamedRelPath(_, patternRels) =>
      ProjectedPath.singleNodeProjector(patternRels.head.left.name, relSeqProjector(patternRels, tail))
  }

  def relSeqProjector(patternRels: Seq[PatternRelationship], tail: PathProjector): PathProjector =
    patternRels.foldRight(tail)(relProjector)

  def relProjector(patternRel: PatternRelationship, tail: PathProjector): PathProjector = patternRel match {
    case PatternRelationship(IdName(rel), _, Direction.INCOMING, _, SimplePatternLength) =>
      ProjectedPath.singleIncomingRelationshipProjector(rel, tail)

    case PatternRelationship(IdName(rel), _, _, _, SimplePatternLength) =>
      ProjectedPath.singleOutgoingRelationshipProjector(rel, tail)

    case PatternRelationship(IdName(rel), _, Direction.INCOMING, _, VarPatternLength(_, _)) =>
      ProjectedPath.varLengthIncomingRelationshipProjector(rel, tail)

    case PatternRelationship(IdName(rel), _, _, _, VarPatternLength(_, _)) =>
      ProjectedPath.varLengthOutgoingRelationshipProjector(rel, tail)
  }
}

