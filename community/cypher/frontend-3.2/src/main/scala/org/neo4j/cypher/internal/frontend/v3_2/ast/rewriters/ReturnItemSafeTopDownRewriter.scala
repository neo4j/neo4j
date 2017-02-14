package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_2.Foldable.TreeAny
import org.neo4j.cypher.internal.frontend.v3_2.ast.{AliasedReturnItem, Expression}
import org.neo4j.cypher.internal.frontend.v3_2.{InternalException, Rewriter}

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
