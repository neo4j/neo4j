/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

trait Spatula {

  def execute(task: Task): QueryExecution
}

trait Task {

  def executeWorkUnit(): Seq[Task]
  def canContinue: Boolean
}

trait ExecutableQuery {
  def initialTask(): Task
}

trait QueryExecution {
  def await(): Option[Throwable]
}

class ASpatula(executor: Executor) extends Spatula {

  private val fishslice = new ExecutorCompletionService[Try[TaskResult]](executor)

  override def execute(task: Task): QueryExecution = new TheQueryExecution(schedule(task), this)

  def schedule(task: Task): Future[Try[TaskResult]] = {
    def wrapMe(task: Task) : Callable[Try[TaskResult]] = {
      new Callable[Try[TaskResult]] {
        override def call(): Try[TaskResult] =
          Try(TaskResult(task, task.executeWorkUnit()))
      }
    }
    fishslice.submit(wrapMe(task))
  }

  class TheQueryExecution(initialTask: Future[Try[TaskResult]], spatula: ASpatula) extends QueryExecution {

    var inFlightTasks = new ArrayBuffer[Future[Try[TaskResult]]]
    inFlightTasks += initialTask

    override def await(): Option[Throwable] = {

      while (inFlightTasks.nonEmpty) {
        val newInFlightTasks = new ArrayBuffer[Future[Try[TaskResult]]]
        for (future <- inFlightTasks) {
          val taskResultTry = future.get(30, TimeUnit.SECONDS)
          taskResultTry match {
            case Success(taskResult) =>
              for (newTask <- taskResult.newDownstreamTasks)
                newInFlightTasks += spatula.schedule(newTask)

              if (taskResult.task.canContinue)
                newInFlightTasks += spatula.schedule(taskResult.task)

            case Failure(exception) =>
              return Some(exception)
          }
        }
        inFlightTasks = newInFlightTasks
      }
      None
    }
  }

}

case class TaskResult(task: Task, newDownstreamTasks: Seq[Task])
