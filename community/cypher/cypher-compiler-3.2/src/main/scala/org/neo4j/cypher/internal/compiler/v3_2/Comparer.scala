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
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_2.spi.{NodeIdWrapper, RelationshipIdWrapper}
import org.neo4j.cypher.internal.compiler.v3_2.util.CompiledOrderabilityUtils
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.JavaConverters._

/**
 * Comparer is a trait that enables it's subclasses to compare to AnyRef with each other.
 */
trait Comparer extends CypherSerializer {

  import Comparer._

  def compareForOrderability(operator: Option[String], l: Any, r: Any)(implicit qtx: QueryState): Int = {
    CompiledOrderabilityUtils.compare(makeComparable(l), makeComparable(r))
  }

  private def makeComparable(a: Any) = a match {
    case n: Node => new NodeIdWrapper {
      override def id() = n.getId
    }
    case r: Relationship => new RelationshipIdWrapper {
      override def id() = r.getId
    }
    case s: Seq[_] => s.asJava
    case m: Map[_, _] => m.asJava
    case x => x
  }

  def compareForComparability(operator: Option[String], l: Any, r: Any)(implicit qtx: QueryState): Option[Int] = {
    try {
      if ((isString(l) && isString(r)) || (isNumber(l) && isNumber(r)) || (isBoolean(l) && isBoolean(r)))
        Some(CypherOrdering.DEFAULT.compare(l, r))
      else
        None
    } catch {
      case _: IllegalArgumentException =>
        None
    }
  }
}

object Comparer {
  def isString(value: Any): Boolean = value match {
    case _: String => true
    case _: Character => true
    case _ => value == null
  }

  def isNumber(value: Any): Boolean = value match {
    case _: Number => true
    case _ => value == null
  }

  def isBoolean(value: Any): Boolean = value match {
    case _: Boolean => true
    case _ => value == null
  }
}
