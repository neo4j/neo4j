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

  sealed trait Reason {
    def maybeConflict: Option[Conflict]
  }

  case object Unknown extends Reason {
    override def maybeConflict: Option[Conflict] = None
  }

  case object UpdateStrategyEager extends Reason {
    override def maybeConflict: Option[Conflict] = None
  }

  case class LabelReadSetConflict(label: LabelName, override val maybeConflict: Option[Conflict] = None)
      extends Reason

  case class LabelReadRemoveConflict(label: LabelName, override val maybeConflict: Option[Conflict] = None)
      extends Reason

  case class ReadDeleteConflict(identifier: String, override val maybeConflict: Option[Conflict] = None)
      extends Reason

  case class ReadCreateConflict(override val maybeConflict: Option[Conflict] = None) extends Reason

  case class PropertyReadSetConflict(
    property: PropertyKeyName,
    override val maybeConflict: Option[Conflict] = None
  ) extends Reason

  case class UnknownPropertyReadSetConflict(override val maybeConflict: Option[Conflict] = None) extends Reason

  case object WriteAfterCallInTransactions extends Reason {
    override def maybeConflict: Option[Conflict] = None
  }
}
