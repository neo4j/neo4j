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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues.list
import org.neo4j.values.virtual._
import org.scalatest.matchers.{MatchResult, Matcher}

object ValueComparisonHelper {

  def beEquivalentTo(result: Seq[Map[String, Any]]) = new Matcher[Seq[ExecutionContext]] {
    override def apply(left: Seq[ExecutionContext]): MatchResult = MatchResult(
      matches = left.indices.forall(i =>
                                      left(i).keySet == result(i).keySet &&
                                        left(i).forall{
                                          case (k,v) => check(v, result(i)(k))
                                        }),
      rawFailureMessage = s"$left != $result",
      rawNegatedFailureMessage = s"$left == $result")
  }

  def beEquivalentTo(value: Any) = new Matcher[AnyValue] {
    override def apply(left: AnyValue): MatchResult = MatchResult(
      matches = check(left, value),
      rawFailureMessage = s"$left != $value",
      rawNegatedFailureMessage = s"$left == $value")
  }


  private def check(left: AnyValue, right: Any): Boolean = (left, right) match {
    case (l: AnyValue, r: AnyValue) => l == r
    case (l: AnyValue, null) => l == Values.NO_VALUE
    case (n1: NodeValue, n2: Node) => n1.id() == n2.getId
    case (e: EdgeValue, r: Relationship) => e.id() == r.getId
    case (l: ListValue, s: Seq[_]) if l.size() == s.size =>  s.indices.forall(i => check(l.value(i), s(i)))
    case (l: MapValue, s: Map[_, _]) if l.size() == s.size =>
      val map = s.asInstanceOf[Map[String, Any]]
      l.keys() == list(map.keys.map(k => stringValue(k)).toArray:_*) && map.keys.forall(k => check(l.get(k), map(k)))
    case (sv: TextValue, v) => sv.stringValue() == v
    case (lv: LongValue, v) => lv.longValue() == v
    case (iv: IntValue, v) => iv.value() == v
    case (dv: DoubleValue, v) => dv.value() == v

    case (l, r) => l == r
  }
}
