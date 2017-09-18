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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.frontend.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.logical.plans.{DoNotIncludeTies, Limit, Sort, Top}

/**
  * When doing ORDER BY c1,c2,...,cn LIMIT e, we don't have to sort the full result in one go
  */
case object useTop extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o @ Limit(Sort(src, sortDescriptions), limit, DoNotIncludeTies) =>
      Top(src, sortDescriptions, limit)(o.solved)
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
