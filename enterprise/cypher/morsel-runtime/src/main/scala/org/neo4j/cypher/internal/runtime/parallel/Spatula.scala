package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent._

import scala.collection.mutable.ArrayBuffer

trait Spatula {

  def execute(task: Task): QueryExecution
}

trait Task {

  def executeWorkUnit(): Seq[Task]
  def canContinue(): Boolean
}

trait ExecutableQuery {
  def initialTask(): Task
}

trait QueryExecution {
  def await(): Unit
}

class ASpatula(val concurrency: Int) extends Spatula {

  private val fishslice = new ExecutorCompletionService[TaskResult](Executors.newFixedThreadPool(concurrency))

  override def execute(task: Task): QueryExecution = new TheQueryExecution(schedule(task), this)

  def schedule(task: Task): Future[TaskResult] = {
    def wrapMe(task: Task) : Callable[TaskResult] = {
      new Callable[TaskResult] {
        override def call(): TaskResult =
          TaskResult(task, task.executeWorkUnit())
      }
    }
    fishslice.submit(wrapMe(task))
  }

  class TheQueryExecution(initialTask: Future[TaskResult], spatula: ASpatula) extends QueryExecution {

    var inFlightTasks = new ArrayBuffer[Future[TaskResult]]
    inFlightTasks += initialTask

    override def await(): Unit = {

      while (inFlightTasks.nonEmpty) {
        val newInFlightTasks = new ArrayBuffer[Future[TaskResult]]
        for (future <- inFlightTasks) {
          val taskResult = future.get(30, TimeUnit.SECONDS)
          for (newTask <- taskResult.newDownstreamTasks)
            newInFlightTasks += spatula.schedule(newTask)

          if (taskResult.task.canContinue())
            newInFlightTasks += spatula.schedule(taskResult.task)
        }
        inFlightTasks = newInFlightTasks
      }
    }
  }

}

case class TaskResult(task: Task, newDownstreamTasks: Seq[Task])
