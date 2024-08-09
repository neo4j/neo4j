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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ArrayValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.IntValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues.list
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

object ValueComparisonHelper {

  def beEquivalentTo(result: Seq[Map[String, Any]]): Matcher[Seq[CypherRow]] = new Matcher[Seq[CypherRow]] {

    override def apply(left: Seq[CypherRow]): MatchResult = MatchResult(
      matches = result.size == left.size && left.indices.forall(i => {
        val res = result(i)
        val row = left(i)
        res.size == row.numberOfColumns &&
        res.keySet.forall(row.containsName) &&
        res.forall {
          case (k, v) => check(row.getByName(k), v)
        }
      }),
      rawFailureMessage = s"$left != $result",
      rawNegatedFailureMessage = s"$left == $result"
    )
  }

  def beEquivalentTo(value: Any): Matcher[AnyValue] = new Matcher[AnyValue] {

    override def apply(left: AnyValue): MatchResult = MatchResult(
      matches = check(left, value),
      rawFailureMessage = s"$left != $value",
      rawNegatedFailureMessage = s"$left == $value"
    )
  }

  private def check(left: AnyValue, right: Any): Boolean = (left, right) match {
    case (l: AnyValue, r: AnyValue)                            => l == r
    case (l: AnyValue, null)                                   => l == Values.NO_VALUE
    case (n1: NodeValue, n2: Node)                             => n1.id() == n2.getId
    case (rv: RelationshipValue, r: Relationship)              => rv.id() == r.getId
    case (l: ListValue, s: Seq[_]) if l.actualSize() == s.size => s.indices.forall(i => check(l.value(i), s(i)))
    case (l: MapValue, s: Map[_, _]) if l.size() == s.size =>
      val map = s.asInstanceOf[Map[String, Any]]
      l.keys() == list(map.keys.map(k => stringValue(k)).toArray: _*) && map.keys.forall(k => check(l.get(k), map(k)))
    case (sv: TextValue, v)   => sv.stringValue() == v
    case (lv: LongValue, v)   => lv.longValue() == v
    case (iv: IntValue, v)    => iv.value() == v
    case (dv: DoubleValue, v) => dv.value() == v
    case (a: ArrayValue, v)   => a == Values.of(v)

    case (l, r) => l == r
  }
}
