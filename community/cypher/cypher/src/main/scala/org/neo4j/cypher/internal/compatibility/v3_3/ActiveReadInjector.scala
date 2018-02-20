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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.util.v3_4.attribution.{Attributes, SameId}
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.logical.plans._

/**
  * Detect logical plan parts derived from MERGE, and inject ActiveRead operators above the
  * merge read parts.
  */
case class ActiveReadInjector(attributes: Attributes) extends Rewriter {
  private val instance = bottomUp(Rewriter.lift {

    // single merge node without on match:
    //    MERGE (a)
    case antiCondApply@AntiConditionalApply(
          optional@Optional(mergeReadPart, protectedSymbols),
          createBranch,
          items
        ) if hasCreateMerge(createBranch) =>
            AntiConditionalApply(
              Optional(
                ActiveRead(mergeReadPart)(attributes.copy(optional.id)),
                protectedSymbols
              )(SameId(optional.id)),
              createBranch,
              items
            )(SameId(antiCondApply.id))

    // single merge node with on match:
    //    MERGE (a) ON MATCH SET n.prop = 2
    case antiCondApply@AntiConditionalApply(
          condApply@ConditionalApply(
            optional@Optional(mergeReadPart, protectedSymbols),
            onMatch,
            condItems
          ),
          createBranch,
          antiCondItems
        ) if hasCreateMerge(createBranch) =>
      AntiConditionalApply(
        ConditionalApply(
          Optional(
            ActiveRead(mergeReadPart)(attributes.copy(optional.id)),
            protectedSymbols
          )(SameId(optional.id)),
          onMatch,
          condItems
        )(SameId(condApply.id)),
        createBranch,
        antiCondItems
      )(SameId(antiCondApply.id))
  })

  private def hasCreateMerge(plan:LogicalPlan): Boolean = {
    var p = plan
    var found = false
    while (!found && p != null) {
      p match {
        case x: MergeCreateNode => found = true
        case x: MergeCreateRelationship => found = true
        case withSource if p.rhs.nonEmpty => throw new UnsupportedOperationException("not implemented")
        case withSource if p.lhs.nonEmpty => p = withSource.lhs.get
        case _ => p = null
      }
    }
    found
  }

  def apply(that: AnyRef): AnyRef = instance.apply(that)
}
