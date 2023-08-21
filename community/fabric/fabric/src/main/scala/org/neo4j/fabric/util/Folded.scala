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
package org.neo4j.fabric.util

import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings

object Folded {

  sealed trait Instruction[R]

  case class Stop[R](r: R) extends Instruction[R]

  case class Descend[R](r: R) extends Instruction[R]

  case class DescendWith[R](r1: R, r2: R) extends Instruction[R]

  implicit class FoldableOps[T](f: T) {

    def folded[R](init: R)(merge: (R, R) => R)(instructions: PartialFunction[Any, Instruction[R]]): R =
      f.folder.treeFold(init) {
        case a: Any if instructions.isDefinedAt(a) =>
          (r: R) =>
            instructions(a) match {
              case i: Stop[R]        => SkipChildren(merge(r, i.r))
              case i: Descend[R]     => TraverseChildren(merge(r, i.r))
              case i: DescendWith[R] => TraverseChildrenNewAccForSiblings(merge(r, i.r1), rb => merge(rb, i.r2))
            }
      }
  }
}
