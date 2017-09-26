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
///*
// * Copyright (c) 2002-2017 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.cypher.internal.runtime.vectorized
//
//import org.neo4j.cypher.internal.frontend.v3_3.InternalException
//import org.neo4j.cypher.internal.spi.v3_3.QueryContext
//import org.neo4j.graphdb.Result
//import org.neo4j.values.virtual.MapValue
//
//import scala.collection.mutable
//
//class Executor(lane: Pipeline,
//               queryContext: QueryContext,
//               params: MapValue) {
//
//  private val MORSEL_SIZE = 10
//
//  def accept[E <: Exception](visitor: Result.ResultVisitor[E]): Unit = {
//    val state = new QueryState(params = params)
//    val query = Query(lane, visitor, queryContext, state, lane.slotInformation, Thread.currentThread().getId.toString)
//
//    val workQueue = new mutable.Queue[Task]()
//    val pipelines = new mutable.Stack[Pipeline]
//    val pipelineCounter = new mutable.HashMap[Pipeline, Int]()
//    pipelines.push(lane)
//
//    def queueWork(task: Task): Unit = {
//      val current = pipelineCounter.getOrElse(task.pipeline, 0)
//      pipelineCounter(task.pipeline) = current + 1
//      workQueue.enqueue(task)
//    }
//
//    // Queue up all leafs that we can start on
//    while (pipelines.nonEmpty) {
//      val current = pipelines.pop()
//      if (current.dependency.nonEmpty)
//        current.dependency.foreach(pipelines.push)
//      else
//        queueWork(new Task(current, query, Init))
//    }
//
//    // Now execute tasks until we are done
//    while(workQueue.nonEmpty) {
//      val task = workQueue.dequeue()
//      val context = task.query.context
//      val state = task.query.state
//      val morsel = Morsel.create(task.pipeline.slotInformation, MORSEL_SIZE)
//      val (returnType, continue) = task.pipeline.operate(task.continue, morsel, context, state)
//
//      (task.pipeline.parent, returnType) match {
//        case (None, MorselType) =>
//          val resultRow = new MorselResultRow(morsel, 0, task.query.resultPipe, context)
//          (0 until morsel.validRows) foreach { position =>
//            resultRow.currentPos = position
//            task.query.visitor.visit(resultRow)
//          }
//        case (None, UnitType) =>
//        // Empty on purpose
//
//        case (Some(parent), MorselType) =>
//          queueWork(new Task(parent, task.query, InitWithData(morsel)))
//
//        case (Some(_), UnitType) =>
//          throw new InternalException("something went wrong dispatching work for this query")
//      }
//
//      continue match {
//        case Done =>
//          val i = pipelineCounter(task.pipeline)
//          pipelineCounter(task.pipeline) = i - 1
//          if (i == 0) {
//            task.pipeline.parent match {
//              case None =>
//                println("finished!")
//                // we are done!
//              case Some(parent) =>
//                val closingTask = new Task(parent, task.query, Done)
//                queueWork(closingTask)
//            }
//          }
//
//        case _ => queueWork(new Task(task.pipeline, query, continue))
//      }
//    }
//  }
//}
