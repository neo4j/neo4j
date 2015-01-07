/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes.matching

import org.neo4j.cypher.internal.compiler.v1_9.commands.Predicate
import collection.immutable.Set
import collection.Seq
import collection.Map
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState

/*
Normally, when we encounter an optional relationship, we can try with null and see if that's enough. But for
double optional patterns ( a -[?]-> X <-[?]- b ), we have to try to get to X both from a and from b
to find all combinations.

This class takes care of double optional patterns
 */
class DoubleOptionalPatternMatcher(bindings: Map[String, MatchingPair],
                                   predicates: Seq[Predicate],
                                   includeOptionals: Boolean,
                                   source: ExecutionContext,
                                   state: QueryState,
                                   doubleOptionalPaths: Seq[DoubleOptionalPath])
  extends PatternMatcher(bindings, predicates, includeOptionals, source, state) {

  override protected def traverseNextSpecificNode[U](remaining: Set[MatchingPair],
                                                     history: History,
                                                     yielder: ExecutionContext => U,
                                                     current: MatchingPair,
                                                     alreadyInExtraWork: Boolean): Boolean = {
    val initialResult = super.traverseNextSpecificNode(remaining, history, yielder, current, alreadyInExtraWork = false)

    // To prevent going around for infinity, we check that we are not already checking for double optionals
    if (includeOptionals && !alreadyInExtraWork) {
      val pathsToCheck: List[DoubleOptionalPath] = shouldDoExtraWork(current, remaining).toList

      val extendedCheck = pathsToCheck.foldLeft(initialResult)((last, next) => {
        val (newRemaining: Set[MatchingPair], newCurrent: MatchingPair) = swap(remaining, current, next)

        val myYielder = createYielder(yielder, next, newCurrent) _

        debug(newRemaining, newCurrent, history, next)
        traverseNextSpecificNode(newRemaining, history, myYielder, newCurrent, alreadyInExtraWork = true) || last
      })

      extendedCheck
    }
    else
      initialResult
  }

  private def debug(remaining: Set[MatchingPair], current: MatchingPair, history: History, dop: DoubleOptionalPath) {
    if (isDebugging) {

      println(String.format("""DoubleOptionalPatternMatcher.traverseNextSpecificNode -- Extra check
remaining = %s
current   = %s
history   = %s
DoubleOP  = %s
    """, remaining, current, history, dop))
    }
  }

  private def swap(remaining: Set[MatchingPair], current: MatchingPair, dop: DoubleOptionalPath): (Set[MatchingPair], MatchingPair) =
    (remaining, remaining.find(_.patternElement.key == dop.otherNode(current.patternElement.key)).get)

  private def shouldDoExtraWork(current: MatchingPair, remaining: Set[MatchingPair]): Seq[DoubleOptionalPath] =
    doubleOptionalPaths.filter(_.shouldDoWork(current.patternNode.key, remaining))


  /*
  We're only looking for paths that we would not find during our normal pattern matching. This yielder protects us
  from yielding subgraphs that have already been found.
   */
  private def createYielder[A](inner: ExecutionContext => A, dop: DoubleOptionalPath, current: MatchingPair)(m: ExecutionContext) {
    val Relationships(closestRel, oppositeRel) = dop.relationshipsSeenFrom(current.patternElement.key)

    val weShouldYield = m.get(closestRel) != Some(null) && m.get(oppositeRel) == Some(null)

    if (weShouldYield) {
      inner(m)

      if (isDebugging) println(String.format("""optional extra yield:
      m=%s
""", m))

    }

  }
}
