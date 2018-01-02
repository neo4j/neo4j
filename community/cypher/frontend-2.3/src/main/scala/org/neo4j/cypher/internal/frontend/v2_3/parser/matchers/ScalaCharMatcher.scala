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
package org.neo4j.cypher.internal.frontend.v2_3.parser.matchers

import org.parboiled.MatcherContext
import org.parboiled.matchers.CustomMatcher

abstract class ScalaCharMatcher(label: String) extends CustomMatcher(label) {

  protected def matchChar(c: Char): Boolean

  def `match`[V](context: MatcherContext[V]): Boolean =
    if (matchChar(context.getCurrentChar)) {
      context.advanceIndex(1)
      context.createNode()
      true
    } else {
      false
    }

  def isSingleCharMatcher: Boolean = true

  def canMatchEmpty: Boolean = false

  def isStarterChar(c: Char): Boolean = matchChar(c)

  def getStarterChar: Char = 'a'
}
