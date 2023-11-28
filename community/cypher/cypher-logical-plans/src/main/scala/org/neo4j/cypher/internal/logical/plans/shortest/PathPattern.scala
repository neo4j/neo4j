/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans.shortest

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.util.Rewritable

// Note, this is a copy of org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
// We can probably unify them.
case class ShortestRelationshipPattern(
  name: Option[LogicalVariable],
  rel: PatternRelationship,
  single: Boolean
)(
  val expr: ShortestPathsPatternPart
) extends Rewritable {
  def availableSymbols: Set[LogicalVariable] = rel.availableSymbols() ++ name

  override def dup(children: Seq[AnyRef]): this.type =
    copy(
      children(0).asInstanceOf[Option[LogicalVariable]],
      children(1).asInstanceOf[PatternRelationship],
      children(2).asInstanceOf[Boolean]
    )(expr).asInstanceOf[this.type]
}

object ShortestRelationshipPattern {

  def from(pattern: org.neo4j.cypher.internal.ir.ShortestRelationshipPattern): ShortestRelationshipPattern = {
    ShortestRelationshipPattern(
      name = pattern.maybePathVar,
      rel = PatternRelationship.from(pattern.rel),
      single = pattern.single
    )(pattern.expr)
  }
}

// Note, this is a copy of org.neo4j.cypher.internal.ir.PatternRelationship
// We can probably unify them.
case class PatternRelationship(
  name: LogicalVariable,
  nodes: (LogicalVariable, LogicalVariable),
  dir: SemanticDirection,
  types: Seq[RelTypeName],
  length: PatternLength
) {
  def availableSymbols(): Set[LogicalVariable] = Set(name, nodes._1, nodes._2)
}

object PatternRelationship {

  def from(pattern: org.neo4j.cypher.internal.ir.PatternRelationship): PatternRelationship = {
    PatternRelationship(
      name = pattern.variable,
      nodes = (pattern.left, pattern.right),
      dir = pattern.dir,
      types = pattern.types,
      length = pattern.length
    )
  }
}
