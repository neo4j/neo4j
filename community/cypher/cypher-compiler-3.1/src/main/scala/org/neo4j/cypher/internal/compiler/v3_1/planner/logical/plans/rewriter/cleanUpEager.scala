/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_1.{Rewriter, bottomUp}

case object cleanUpEager extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {

    // E E L => E L
    case eager@Eager(Eager(source)) =>
      eager.copy(inner = source)(eager.solved)

    // E U => U E
    case eager@Eager(unwind@UnwindCollection(source, _, _)) =>
      unwind.copy(left = eager.copy(inner = source)(eager.solved))(eager.solved)

    // E LCSV => LCSV E
    case eager@Eager(loadCSV@LoadCSV(source, _, _, _, _, _)) =>
      loadCSV.copy(source = eager.copy(inner = source)(eager.solved))(eager.solved)
  })

  override def apply(input: AnyRef) = instance.apply(input)
}
