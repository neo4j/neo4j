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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

case class Modulo(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def calc(a: Number, b: Number): Any = (a, b) match {
    case (l1: java.lang.Double, _) => l1 % b.doubleValue()
    case (_, l2: java.lang.Double) => a.doubleValue() % l2
    case (l1: java.lang.Float, _) => l1 % b.floatValue()
    case (_, l2: java.lang.Float) => a.floatValue() % l2

    //no floating point values, then we treat everything else as longs
    case _ => a.longValue() % b.longValue()
  }


  def rewrite(f: (Expression) => Expression) = f(Modulo(a.rewrite(f), b.rewrite(f)))

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
