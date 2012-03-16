/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.cypher.internal.commands.Predicate
import collection.immutable.Set
import collection.Seq
import collection.Map

class DoubleOptionalPatternMatcher(bindings: Map[String, MatchingPair],
                                   predicates: Seq[Predicate],
                                   includeOptionals: Boolean,
                                   source: Map[String, Any],
                                   doubleOptionalPaths: Seq[DoubleOptionalPath])
  extends PatternMatcher(bindings, predicates, includeOptionals, source) {

  override protected def traverseNextSpecificNode[U](remaining: Set[MatchingPair],
                                                     history: History,
                                                     yielder: (Map[String, Any]) => U,
                                                     current: MatchingPair,
                                                     leftToDoAfterThisOne: Set[MatchingPair],
                                                     alreadyInExtraWork: Boolean): Boolean = {
    val result = super.traverseNextSpecificNode(remaining, history, yielder, current, leftToDoAfterThisOne, false)

    if (includeOptionals && !alreadyInExtraWork) {
      val work = shouldDoExtraWork(current, remaining)
      work.foldLeft(result)((last, next) => {
        val (newHead, newRemaining) = remaining.partition(p => p.patternNode.key == next.endNode)
        val remainingPlusCurrent = newRemaining + current

        val myYielder = (m: Map[String, Any]) => {
          m.get(next.relationshipName) match {
            case Some(null) => yielder(m)
            case _ =>
          }
        }

        traverseNextSpecificNode(remaining, history, myYielder, newHead.head, remainingPlusCurrent, true) || last
      })
    }
    else
      result
  }

  def shouldDoExtraWork(current: MatchingPair, remaining: Set[MatchingPair]): Seq[DoubleOptionalPath] = doubleOptionalPaths.filter(
    dop => {
      val b = dop.startNode == current.patternNode.key
      val sdf = remaining.exists(_.patternNode.key == dop.endNode)
      b && sdf
    }
  )
}

case class DoubleOptionalPath(startNode:String, endNode:String, relationshipName:String)