/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.util.v3_4.attribution.{Attributes, SameId}
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.logical.plans._

/**
  * Detect logical plan parts derived from MERGE, and inject ActiveRead operators above the
  * merge read parts.
  */
case class ActiveReadInjector(attributes: Attributes) {
  private val instance = bottomUp(Rewriter.lift {

    // single merge node:           MERGE (a)
    // unbound relationship merge:  MERGE (a:A)-[r:R]->(b)
    case antiCondApply@AntiConditionalApply(
          optional@Optional(mergeReadPart, protectedSymbols),
          onCreate,
          items
        ) if hasCreateMerge(onCreate) =>
            AntiConditionalApply(
              Optional(
                ActiveRead(mergeReadPart)(attributes.copy(optional.id)),
                protectedSymbols
              )(SameId(optional.id)),
              onCreate,
              items
            )(SameId(antiCondApply.id))

    // single merge node with on-match:         MERGE (a) ON MATCH SET n.prop = 2
    // unbound relationship merge w. on-match:  MERGE (a:A)-[r:R]->(b) ON MATCH SET r.prop = 3
    case antiCondApply@AntiConditionalApply(
          condApply@ConditionalApply(
            optional@Optional(mergeReadPart, protectedSymbols),
            onMatch,
            condItems
          ),
          onCreate,
          antiCondItems
        ) if hasCreateMerge(onCreate)
    =>
      AntiConditionalApply(
        ConditionalApply(
          Optional(
            ActiveRead(mergeReadPart)(attributes.copy(optional.id)),
            protectedSymbols
          )(SameId(optional.id)),
          onMatch,
          condItems
        )(SameId(condApply.id)),
        onCreate,
        antiCondItems
      )(SameId(antiCondApply.id))

    // bound relationship merge:  MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)
    case antiCondOuter@AntiConditionalApply(
          antiCondInner@AntiConditionalApply(
            optionalRead:Optional,
            optionalLockedRead:Optional,
            condItemsInner
          ),
          onCreate,
          condItemsOuter
        ) if hasCreateMerge(onCreate) && hasLockNodes(optionalLockedRead.source)
    =>
      AntiConditionalApply(
        AntiConditionalApply(
          Optional(
            ActiveRead(optionalRead.source)(attributes.copy(optionalRead.id)),
            optionalRead.protectedSymbols
          )(SameId(optionalRead.id)),
          Optional(
            ActiveRead(optionalLockedRead.source)(attributes.copy(optionalLockedRead.id)),
            optionalLockedRead.protectedSymbols
          )(SameId(optionalLockedRead.id)),
          condItemsInner
        )(SameId(antiCondInner.id)),
        onCreate,
        condItemsOuter
      )(SameId(antiCondOuter.id))

    // bound relationship merge w. on-match:
    //        MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m) ON MATCH SET r.prop = 3
    case antiCondOuter@AntiConditionalApply(
          cond@ConditionalApply(
            antiCondInner@AntiConditionalApply(
              optionalRead:Optional,
              optionalLockedRead:Optional,
              antiCondItemsInner
            ),
            onMatch,
            condItems
          ),
          onCreate,
          antiCondItemsOuter
        ) if hasCreateMerge(onCreate) && hasLockNodes(optionalLockedRead.source)
    =>
      AntiConditionalApply(
        ConditionalApply(
          AntiConditionalApply(
            Optional(
              ActiveRead(optionalRead.source)(attributes.copy(optionalRead.id)),
              optionalRead.protectedSymbols
            )(SameId(optionalRead.id)),
            Optional(
              ActiveRead(optionalLockedRead.source)(attributes.copy(optionalLockedRead.id)),
              optionalLockedRead.protectedSymbols
            )(SameId(optionalLockedRead.id)),
            antiCondItemsInner
          )(SameId(antiCondInner.id)),
          onMatch,
          condItems
        )(SameId(cond.id)),
        onCreate,
        antiCondItemsOuter
      )(SameId(antiCondOuter.id))
  })

  private def hasCreateMerge(plan:LogicalPlan): Boolean = {
    plan.treeExists {
      case _: MergeCreateNode => true
      case _: MergeCreateRelationship =>  true
    }
  }

  private def hasLockNodes(plan:LogicalPlan): Boolean =
    plan.treeExists {
      case _:LockNodes => true
    }

  def apply(that: LogicalPlan): LogicalPlan = instance.apply(that).asInstanceOf[LogicalPlan]
}
