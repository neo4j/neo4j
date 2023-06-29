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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.attribution.Id

object EagernessReason {

  case class Conflict(first: Id, second: Id) extends Rewritable {

    override def dup(children: Seq[AnyRef]): this.type = {
      val a = children(0).asInstanceOf[Id]
      val b = children(1).asInstanceOf[Id]
      if (a != first || b != second) {
        Conflict(a, b).asInstanceOf[this.type]
      } else {
        this
      }
    }
  }

  sealed trait Reason

  case object Unknown extends Reason
  case object UpdateStrategyEager extends Reason
  case object WriteAfterCallInTransactions extends Reason

  /**
   * Non-unique reasons can be reported by multiple conflicting plan pairs.
   */
  sealed trait NonUnique extends Reason {
    def withConflict(conflict: Conflict): ReasonWithConflict = ReasonWithConflict(this, conflict)

    def withMaybeConflict(maybeConflict: Option[Conflict]): Reason = {
      maybeConflict.fold(this: Reason)(withConflict)
    }

  }

  case class LabelReadSetConflict(label: LabelName) extends NonUnique

  object LabelReadSetConflict {

    def apply(label: LabelName, maybeConflict: Option[Conflict]): Reason =
      LabelReadSetConflict(label).withMaybeConflict(maybeConflict)
  }

  case class TypeReadSetConflict(relType: RelTypeName) extends NonUnique

  object TypeReadSetConflict {

    def apply(relType: RelTypeName, maybeConflict: Option[Conflict]): Reason =
      TypeReadSetConflict(relType).withMaybeConflict(maybeConflict)
  }

  case class LabelReadRemoveConflict(label: LabelName) extends NonUnique

  object LabelReadRemoveConflict {

    def apply(label: LabelName, maybeConflict: Option[Conflict]): Reason =
      LabelReadRemoveConflict(label).withMaybeConflict(maybeConflict)
  }

  case class ReadDeleteConflict(identifier: String) extends NonUnique

  object ReadDeleteConflict {

    def apply(identifier: String, maybeConflict: Option[Conflict]): Reason =
      ReadDeleteConflict(identifier).withMaybeConflict(maybeConflict)
  }

  case class PropertyReadSetConflict(property: PropertyKeyName) extends NonUnique

  object PropertyReadSetConflict {

    def apply(property: PropertyKeyName, maybeConflict: Option[Conflict]): Reason =
      PropertyReadSetConflict(property).withMaybeConflict(maybeConflict)
  }

  case object ReadCreateConflict extends NonUnique {

    def apply(maybeConflict: Option[Conflict]): Reason =
      ReadCreateConflict.withMaybeConflict(maybeConflict)

    def apply(): Reason = this
  }

  case object UnknownPropertyReadSetConflict extends NonUnique {

    def apply(maybeConflict: Option[Conflict]): Reason =
      UnknownPropertyReadSetConflict.withMaybeConflict(maybeConflict)
    def apply(): Reason = this
  }

  final case class ReasonWithConflict(reason: NonUnique, conflict: Conflict) extends Reason

  /**
   * @param summary For a given non-unique reason, contains a single (arbitrary) conflicting plan pair
   *                and a total number of conflicting pairs with the same reason.
   */
  final case class Summarized(summary: Map[NonUnique, (Conflict, Int)])
      extends Reason {

    def addReason(r: ReasonWithConflict): Summarized = {
      copy(summary = summary.updatedWith(r.reason) {
        case None         => Some(r.conflict -> 1)
        case Some(_ -> n) => Some(r.conflict -> (n + 1))
      })
    }
  }

  object Summarized {
    val empty: Summarized = Summarized(Map.empty)
  }
}
