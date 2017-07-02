/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.frontend.v3_3.Foldable._
import org.neo4j.cypher.internal.frontend.v3_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_3.Rewriter

import scala.annotation.tailrec
import scala.collection.mutable

object TopDownWithState {

  private class TopDownWithStateRewriter[STATE](rewriterCreator: STATE => Rewriter, stateChange: AnyRef => Option[STATE]) extends Rewriter {
    override def apply(that: AnyRef): AnyRef = {
      val initialState = stateChange(that).getOrElse(
        throw new IllegalArgumentException("Need the initial object to return an initial state"))

      val initialStack = mutable.ArrayStack((List(that), new mutable.MutableList[AnyRef](), initialState))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.ArrayStack[(List[AnyRef], mutable.MutableList[AnyRef], STATE)]): mutable.MutableList[AnyRef] = {
      val (currentJobs, _, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren, state) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          val (job :: jobs, doneJobs, state) = stack.pop()
          val doneJob = job.dup(newChildren)
          stack.push((jobs, doneJobs += doneJob, state))
          rec(stack)
        }
      } else {
        val (newJob :: jobs, doneJobs, oldState) = stack.pop()
        val newState = stateChange(newJob).getOrElse(oldState)
        val rewriter = rewriterCreator(newState)
        val rewrittenJob = newJob.rewrite(rewriter)
        stack.push((rewrittenJob :: jobs, doneJobs, newState))
        stack.push((rewrittenJob.children.toList, new mutable.MutableList(), newState))
        rec(stack)
      }
    }
  }

  def apply[STATE](rewriter: STATE => Rewriter, stateChange: AnyRef => Option[STATE]): Rewriter =
    new TopDownWithStateRewriter(rewriter, stateChange)
}
