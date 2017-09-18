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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import java.util

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_4.CypherOrdering
import org.neo4j.cypher.internal.compiler.v3_4.common.CypherOrderability
import org.neo4j.cypher.internal.compiler.v3_4.spi.{NodeIdWrapper, RelationshipIdWrapper}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{BooleanValue, NumberValue, TextValue}
import org.neo4j.values.virtual.{EdgeValue, ListValue, MapValue, NodeValue}

import scala.collection.JavaConverters._

/**
  * Comparer is a trait that enables it's subclasses to compare to AnyRef with each other.
  */
trait Comparer extends CypherSerializer {

  import Comparer._

  def compareForOrderability(operator: Option[String], l: AnyValue, r: AnyValue)(implicit qtx: QueryState): Int = {
    CypherOrderability.compare(makeComparable(l), makeComparable(r))
  }

  private def makeComparable(a: AnyValue)(implicit qtx: QueryState): AnyRef = a match {
    case n: NodeValue => new NodeIdWrapper {
      override def id() = n.id()
    }
    case r: EdgeValue => new RelationshipIdWrapper {
      override def id() = r.id
    }
    case x: AnyValue => qtx.query.asObject(x).asInstanceOf[AnyRef]
    case x => x
  }

  def compareForComparability(operator: Option[String], l: AnyValue, r: AnyValue)(implicit qtx: QueryState): Option[Int] = {
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
  def isString(value: AnyValue): Boolean = value match {
    case _: TextValue => true
    case _ => value == null
  }

  def isNumber(value: AnyValue): Boolean = value match {
    case _: NumberValue => true
    case _ => value == null
  }

  def isBoolean(value: AnyValue): Boolean = value match {
    case _: BooleanValue => true
    case _ => value == null
  }
}
