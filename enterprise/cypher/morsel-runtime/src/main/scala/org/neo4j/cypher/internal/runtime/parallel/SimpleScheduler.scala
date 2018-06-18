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

/**
  * A simple implementation of the Scheduler trait
  */
class SimpleScheduler(executor: Executor) extends Scheduler {

  private val executionService = new ExecutorCompletionService[Try[TaskResult]](executor)

  override def execute(task: Task): QueryExecution = new SimpleQueryExecution(schedule(task), this)

  def schedule(task: Task): Future[Try[TaskResult]] = {
    val callableTask =
      new Callable[Try[TaskResult]] {
        override def call(): Try[TaskResult] =
          Try(TaskResult(task, task.executeWorkUnit()))
      }

    executionService.submit(callableTask)
  }

  class SimpleQueryExecution(initialTask: Future[Try[TaskResult]], scheduler: SimpleScheduler) extends QueryExecution {

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
                newInFlightTasks += scheduler.schedule(newTask)

              if (taskResult.task.canContinue)
                newInFlightTasks += scheduler.schedule(taskResult.task)

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
