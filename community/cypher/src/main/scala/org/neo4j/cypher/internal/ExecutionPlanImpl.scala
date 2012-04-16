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
package org.neo4j.cypher.internal

import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import collection.Seq
import java.lang.Iterable
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher.commands._
import org.neo4j.cypher._

class ExecutionPlanImpl(query: Query, graph: GraphDatabaseService) extends ExecutionPlan {
  val (executionPlan, executionPlanText) = prepareExecutionPlan()

  def execute(params: Map[String, Any]): ExecutionResult = {
    val plan = executionPlan(params)
    plan
  }


  private def prepareExecutionPlan(): ((Map[String, Any]) => PipeExecutionResult, String) = {
    query match {
      case Query(returns, start, matching, where, aggregation, sort, slice, namedPaths, queryText) => {
        var sorted = false
        var aggregated = false
        val predicates = where match {
          case None => Seq()
          case Some(w) => w.atoms
        }

        val paramPipe = new ParameterPipe()
        val pipe = createSourcePumps(paramPipe, start.startItems.toList)

        var context = new CurrentContext(pipe, predicates)
        context = addFilters(context)

        context = createMatchPipe(matching, namedPaths, context)

        context.pipe = createShortestPathPipe(context.pipe, matching, namedPaths)
        context = addFilters(context)

        namedPaths match {
          case None =>
          case Some(x) => x.paths.foreach(p => context.pipe = new NamedPathPipe(context.pipe, p))
        }

        if (context.predicates.nonEmpty) {
          context.pipe = new FilterPipe(context.pipe, context.predicates.reduceLeft(_ ++ _))
        }

        val allReturnItems = extractReturnItems(returns, aggregation)

        context.pipe = new ExtractPipe(context.pipe, allReturnItems)

        (aggregation, sort) match {
          case (Some(agg), Some(sorting)) => {
            val sortColumns = sorting.sortItems.map(_.returnItem.columnName)
            val keyColumns = returns.returnItems.map(_.columnName)

            if (canUseOrderedAggregation(sortColumns, keyColumns)) {

              val keyColumnsNotAlreadySorted = returns.
                returnItems.
                filterNot(ri => sortColumns.contains(ri.columnName))
                .map(x => SortItem(x, true))

              val newSort = Some(Sort(sorting.sortItems ++ keyColumnsNotAlreadySorted: _*))

              createSortPipe(newSort, allReturnItems, context)
              context.pipe = new OrderedAggregationPipe(context.pipe, returns.returnItems, agg.aggregationItems)
              sorted = true
              aggregated = true
            }
          }
          case _ =>
        }

        if (!aggregated) {
          aggregation match {
            case None =>
            case Some(aggr) => {
              context.pipe = new EagerAggregationPipe(context.pipe, returns.returnItems, aggr.aggregationItems)
            }
          }
        }

        if (!sorted) {
          createSortPipe(sort, allReturnItems, context)
        }

        slice match {
          case None =>
          case Some(x) => context.pipe = new SlicePipe(context.pipe, x.from, x.limit)
        }

        val returnItems = returns.returnItems ++ aggregation.getOrElse(new Aggregation()).aggregationItems

        val result = new ColumnFilterPipe(context.pipe, returnItems)

        val func = (params: Map[String, Any]) => {
          val start = System.currentTimeMillis()
          val results = result.createResults(params)
          val timeTaken = System.currentTimeMillis() - start

          new PipeExecutionResult(results, result.symbols, returns.columns, timeTaken)
        }
        val executionPlan = result.executionPlan()

        (func, executionPlan)
      }
    }
  }

  private def createSortPipe(sort: Option[Sort], allReturnItems: Seq[ReturnItem], context: CurrentContext) {
    sort match {
      case None =>
      case Some(s) => {

        val sortItems = s.sortItems.map(_.returnItem.concreteReturnItem).filterNot(allReturnItems contains)
        if (sortItems.nonEmpty) {
          context.pipe = new ExtractPipe(context.pipe, sortItems)
        }
        context.pipe = new SortPipe(context.pipe, s.sortItems.toList)
      }
    }
  }

  private def extractReturnItems(returns: Return, aggregation: Option[Aggregation]): Seq[ReturnItem] = {
    val aggregation1 = aggregation.getOrElse(new Aggregation())

    val aggregationItems = aggregation1.aggregationItems.map(_.concreteReturnItem)

    returns.returnItems ++ aggregationItems
  }

  private def addFilters(context: CurrentContext): CurrentContext = {
    if (context.predicates.isEmpty) {
      context
    }
    else {
      val matchingPredicates = context.predicates.filter(x => {

        val unsatisfiedDependencies = x.dependencies.filterNot(context.pipe.symbols contains)
        unsatisfiedDependencies.isEmpty
      })

      if (matchingPredicates.isEmpty) {
        context
      }
      else {
        val filterPredicate = matchingPredicates.reduceLeft(_ ++ _)
        val p = new FilterPipe(context.pipe, filterPredicate)

        new CurrentContext(p, context.predicates.filterNot(matchingPredicates contains))
      }
    }
  }

  private def createMatchPipe(unnamedPaths: Option[Match], namedPaths: Option[NamedPaths], context: CurrentContext): CurrentContext = {
    val namedPattern = namedPaths match {
      case Some(m) => m.paths.flatten
      case None => Seq()
    }

    val unnamedPattern = unnamedPaths match {
      case Some(m) => m.patterns
      case None => Seq()
    }

    (unnamedPattern ++ namedPattern) match {
      case Seq() =>
      case x => context.pipe = new MatchPipe(context.pipe, x, context.predicates)
    }

    context
  }

  private def createShortestPathPipe(source: Pipe, matching: Option[Match], namedPaths: Option[NamedPaths]): Pipe = {
    val unnamedShortestPaths = matching match {
      case Some(m) => m.patterns.filter(_.isInstanceOf[ShortestPath]).map(_.asInstanceOf[ShortestPath])
      case None => Seq()
    }

    val namedShortestPaths = namedPaths match {
      case Some(m) => m.paths.flatMap(_.pathPattern).filter(_.isInstanceOf[ShortestPath]).map(_.asInstanceOf[ShortestPath])
      case None => Seq()
    }

    val shortestPaths = unnamedShortestPaths ++ namedShortestPaths

    var result = source
    shortestPaths.foreach(p => {
      if (p.single)
        result = new SingleShortestPathPipe(result, p)
      else
        result = new AllShortestPathsPipe(result, p)
    })
    result

  }

  private def createSourcePumps(pipe: Pipe, items: List[StartItem]): Pipe = {
    items match {
      case head :: tail => createSourcePumps(createStartPipe(pipe, head), tail)
      case Seq() => pipe
    }
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe = item match {
    case NodeByIndex(varName, idxName, key, value) =>
      new NodeStartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: Iterable[Node] = graph.index.forNodes(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case RelationshipByIndex(varName, idxName, key, value) =>
      new RelationshipStartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: Iterable[Relationship] = graph.index.forRelationships(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case NodeByIndexQuery(varName, idxName, query) =>
      new NodeStartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: Iterable[Node] = graph.index.forNodes(idxName).query(queryText)
        indexHits.asScala
      })

    case RelationshipByIndexQuery(varName, idxName, query) =>
      new RelationshipStartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: Iterable[Relationship] = graph.index.forRelationships(idxName).query(queryText)
        indexHits.asScala
      })

    case NodeById(varName, valueGenerator) => new NodeStartPipe(lastPipe, varName, m => makeNodes[Node](valueGenerator(m), varName, graph.getNodeById))
    case RelationshipById(varName, id) => new RelationshipStartPipe(lastPipe, varName, m => makeNodes[Relationship](id(m), varName, graph.getRelationshipById))
  }

  private def canUseOrderedAggregation(sortColumns: Seq[String], keyColumns: Seq[String]): Boolean = false
  private def makeNodes[T](data: Any, name: String, getElement: Long => T): Seq[T] = {
    def castElement(x: Any): T = x match {
      case i: Int => getElement(i)
      case i: Long => getElement(i)
      case i: String => getElement(i.toLong)
      case element: T => element
    }

    data match {
      case result: Int => Seq(getElement(result))
      case result: Long => Seq(getElement(result))
      case result: java.lang.Iterable[_] => result.asScala.map(castElement).toSeq
      case result: Seq[_] => result.map(castElement).toSeq
      case element: PropertyContainer => Seq(element.asInstanceOf[T])
      case x => throw new ParameterWrongTypeException("Expected a propertycontainer or number here, but got: " + x.toString)
    }
  }

  override def toString = executionPlanText
}

private class CurrentContext(var pipe: Pipe, var predicates: Seq[Predicate])