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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.values._
import org.neo4j.values.storable.{DoubleValue, FloatValue, NumberValue, Values}

case class Modulo(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def calc(a: NumberValue, b: NumberValue): AnyValue = (a, b) match {
    case (l1: DoubleValue, _) => Values.doubleValue(l1.doubleValue() % b.doubleValue())
    case (_, l2: DoubleValue) => Values.doubleValue(a.doubleValue() % l2.doubleValue())
    case (l1: FloatValue, _) => Values.floatValue(l1.value() % b.doubleValue().toFloat)
    case (_, l2: FloatValue) => Values.floatValue(a.doubleValue().toFloat % l2.value())

    //no floating point values, then we treat everything else as longs
    case _ => Values.longValue(a.longValue() % b.longValue())
  }

  def rewrite(f: (Expression) => Expression) = f(Modulo(a.rewrite(f), b.rewrite(f)))

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
