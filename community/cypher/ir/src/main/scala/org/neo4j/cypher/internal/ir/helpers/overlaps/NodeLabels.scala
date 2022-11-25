/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir.helpers.overlaps

/**
 * Represents the set of labels on a given node in the context of solving a node pattern.
 * For example KnownLabels({A,B}) is a solution to (:A&B).
 * SomeUnknownLabels is a more subtle beast, it represent a node containing at least one label that isn't known yet.
 * For example, given the pattern (:!%|%), KnownLabels({}) is the unique solution to !%, and SomeUnknownLabels the synthetic representation of all solutions to %, to be materialised later on.
 * Similarly, evaluating (:%&!(A&B)) gives us the following solutions: {A}, {B}, and SomeUnknownLabels.
 */
sealed trait NodeLabels

object NodeLabels {
  type LabelName = String
  final case class KnownLabels(labelNames: Set[LabelName]) extends NodeLabels
  final case object SomeUnknownLabels extends NodeLabels
}
