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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.Foldable._
import org.neo4j.cypher.internal.frontend.v2_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v2_3.ast.{AliasedReturnItem, Expression}
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, Rewriter}

import scala.annotation.tailrec
import scala.collection.mutable

/*
This rewriter is an alternative to the topDown rewriter that does the same thing,
but does not rewrite ReturnItem alias, only the projected expression
*/
case class ReturnItemSafeTopDownRewriter(inner: Rewriter) extends Rewriter {

  override def apply(that: AnyRef): AnyRef = {
    val initialStack = mutable.ArrayStack((List(that), new mutable.MutableList[AnyRef]()))
    val result = tailrecApply(initialStack)
    assert(result.size == 1)
    result.head
  }

  @tailrec
  private def tailrecApply(stack: mutable.ArrayStack[(List[AnyRef], mutable.MutableList[AnyRef])]): mutable.MutableList[AnyRef] = {
    val (currentJobs, _) = stack.top
    if (currentJobs.isEmpty) {
      val (_, newChildren) = stack.pop()
      if (stack.isEmpty) {
        newChildren
      } else {
        stack.pop() match {
          case (Nil, _) => throw new InternalException("only to stop warnings. should never happen")
          case ((returnItem@AliasedReturnItem(expression, variable)) :: jobs, doneJobs) =>
            val newExpression = newChildren.head.asInstanceOf[Expression]
            val newReturnItem = returnItem.copy(expression = newExpression)(returnItem.position)
            stack.push((jobs, doneJobs += newReturnItem))
          case (job :: jobs, doneJobs) =>
            val doneJob = job.dup(newChildren)
            stack.push((jobs, doneJobs += doneJob))
        }

        tailrecApply(stack)
      }
    } else {
      val (newJob :: jobs, doneJobs) = stack.pop()
      val rewrittenJob = newJob.rewrite(inner)
      stack.push((rewrittenJob :: jobs, doneJobs))
      stack.push((rewrittenJob.children.toList, new mutable.MutableList()))
      tailrecApply(stack)
    }
  }

}
