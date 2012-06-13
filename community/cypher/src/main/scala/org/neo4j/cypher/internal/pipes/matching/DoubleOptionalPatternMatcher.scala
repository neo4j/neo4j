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

/*
Normally, when we encounter an optional relationship, we can try with null and see if that's enough. But for
double optional patterns ( a -[?]-> X <-[?]- b ), we have to try to get to X both from a and from b
to find all combinations.

This class takes care of double optional patterns
 */
class DoubleOptionalPatternMatcher(bindings: Map[String, MatchingPair],
                                   predicates: Seq[Predicate],
                                   includeOptionals: Boolean,
                                   source: Map[String, Any],
                                   doubleOptionalPaths: Seq[DoubleOptionalPath])
  extends PatternMatcher(bindings, predicates, includeOptionals, source) {

  private lazy val optionalRels: Seq[String] = doubleOptionalPaths.map(_.relationshipName)

  override protected def traverseNextSpecificNode[U](remaining: Set[MatchingPair],
                                                     history: History,
                                                     yielder: (Map[String, Any]) => U,
                                                     current: MatchingPair,
                                                     leftToDoAfterThisOne: Set[MatchingPair],
                                                     alreadyInExtraWork: Boolean): Boolean = {
    val initialResult = super.traverseNextSpecificNode(remaining, history, yielder, current, leftToDoAfterThisOne, alreadyInExtraWork = false)

    // To prevent going around for infinity, we check that we are not already checking for double optionals
    if (includeOptionals && !alreadyInExtraWork) {
      val pathsToCheck = shouldDoExtraWork(current, remaining)

      val extendedCheck = pathsToCheck.foldLeft(initialResult)((last, next) => {
        val (newHead, newRemaining) = remaining.partition(p => p.patternNode.key == next.endNode)
        val remainingPlusCurrent = newRemaining + current

        val myYielder = createYielder(yielder, next.relationshipName) _

        traverseNextSpecificNode(remaining, history, myYielder, newHead.head, remainingPlusCurrent, alreadyInExtraWork = true) || last
      })

      extendedCheck
    }
    else
      initialResult
  }


  private def shouldDoExtraWork(current: MatchingPair, remaining: Set[MatchingPair]): Seq[DoubleOptionalPath] = doubleOptionalPaths.filter(
    optionalPath => {
      val standingOnOptionalPath = optionalPath.startNode == current.patternNode.key
      val endPointStillToDo = remaining.exists(_.patternNode.key == optionalPath.endNode)
      standingOnOptionalPath && endPointStillToDo
    })

  private def createYielder[A](inner: Map[String, Any] => A, relName: String)(m: Map[String, Any]) {
    m.get(relName) match {
      case Some(null) if !allOptionalRelsAreNull(m) => inner(m) /*We should only yield if we have found a null for
                                                                  this relationship, and not all optional relationships
                                                                  are null*/
      case _                                        =>
    }
  }

  private def allOptionalRelsAreNull[A](m: Map[String, Any]): Boolean = {
    optionalRels.flatMap(m.get(_)).forall(_ == null)
  }
}

case class DoubleOptionalPath(startNode: String, endNode: String, relationshipName: String)