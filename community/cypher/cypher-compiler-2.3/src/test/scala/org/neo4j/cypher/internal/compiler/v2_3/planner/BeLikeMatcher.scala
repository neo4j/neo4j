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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.scalatest.matchers.{MatchResult, Matcher}

object BeLikeMatcher extends BeLikeMatcher

trait BeLikeMatcher {
  class BeLike(pf: PartialFunction[Object, Unit]) extends Matcher[Object] {

    def apply(left: Object) = {
      MatchResult(
        pf.isDefinedAt(left),
        s"""$left did not match the partial function""",
        s"""$left matched the partial function"""
      )
    }
  }

  def beLike(pf: PartialFunction[Object, Unit]) = new BeLike(pf)
}
