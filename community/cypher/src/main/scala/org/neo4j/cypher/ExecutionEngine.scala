/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher

import commands._
import pipes._
import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import collection.Seq
import java.lang.{Error, Iterable}
import java.util.{Map => JavaMap}


class ExecutionEngine(graph: GraphDatabaseService) {
  checkScalaVersion()

  // This is here because the JavaAPI looks funny with default values
  @throws(classOf[SyntaxException])
  def execute(query: Query): ExecutionResult = execute(query, Map[String, Any]())

  // This is here to support Java people
  @throws(classOf[SyntaxException])
  def execute(query: Query, map: JavaMap[String, Any]): ExecutionResult = execute(query, map.asScala.toMap)

  @throws(classOf[SyntaxException])
  def execute(query: Query, params: Map[String, Any]): ExecutionResult = query match {
    case Query(returns, start, matching, where, aggregation, sort, slice, namedPaths) => {
      val paramPipe = new ParameterPipe(params)
      var pipe = createSourcePumps(paramPipe, start.startItems.toList)

      pipe = createMatchPipe(matching, namedPaths, pipe)

      pipe = createShortestPathPipe(pipe, matching, namedPaths)

      namedPaths match {
        case None =>
        case Some(x) => x.paths.foreach(p => pipe = new NamedPathPipe(pipe, p))
      }

      where match {
        case None =>
        case Some(w) => pipe = new FilterPipe(pipe, w)
      }

      val allReturnItems = extractReturnItems(returns, aggregation, sort)

      pipe = new TransformPipe(pipe, allReturnItems)

      aggregation match {
        case None =>
        case Some(aggr) => {
          pipe = new AggregationPipe(pipe, returns.returnItems, aggr.aggregationItems)
        }
      }

      sort match {
        case None =>
        case Some(s) => {
          pipe = new SortPipe(pipe, s.sortItems.toList)
        }
      }

      slice match {
        case None =>
        case Some(x) => pipe = new SlicePipe(pipe, x.from, x.limit)
      }

      val columns = returns.returnItems ++ aggregation.getOrElse(new Aggregation()).aggregationItems

      val result = new ColumnFilterPipe(pipe, columns) with ExecutionResult

      result
    }
  }

  private def createShortestPathPipe(source: Pipe, matching: Option[Match], namedPaths: Option[NamedPaths]): Pipe = {
    val unnamedShortestPaths = matching match {
      case Some(m) => m.patterns.filter(_.isInstanceOf[ShortestPath]).map(_.asInstanceOf[ShortestPath])
      case None => Seq()
    }

    val namedShortestPaths = namedPaths match {
      case Some(m) => m.paths.flatMap(_.pathPattern ).filter(_.isInstanceOf[ShortestPath]).map(_.asInstanceOf[ShortestPath])
      case None => Seq()
    }

    val shortestPaths = unnamedShortestPaths ++ namedShortestPaths

    var result = source
    shortestPaths.foreach(p => result = new ShortestPathPipe(result, p))
    result

  }

  private def createMatchPipe(unnamedPaths: Option[Match], namedPaths: Option[NamedPaths], pipe: Pipe): Pipe = {
    val namedPattern = namedPaths match {
      case Some(m) => m.paths.flatten
      case None => Seq()
    }

    val unnamedPattern = unnamedPaths match {
      case Some(m) => m.patterns
      case None => Seq()
    }

    (unnamedPattern ++ namedPattern) match {
      case Seq() => pipe
      case x => new MatchPipe(pipe, x)
    }
  }

  private def createSourcePumps(pipe: Pipe, items: List[StartItem]): Pipe = {
    items match {
      case head :: tail => createSourcePumps(createStartPipe(pipe, head), tail)
      case Seq() => pipe
    }
  }

  private def extractReturnItems(returns: Return, aggregation: Option[Aggregation], sort: Option[Sort]): Seq[ReturnItem] = {
    val aggregation1 = aggregation.getOrElse(new Aggregation())
    val sort1 = sort.getOrElse(new Sort())

    val aggregationItems = aggregation1.aggregationItems.map(_.concreteReturnItem)
    val sortItems = sort1.sortItems.map(_.returnItem.concreteReturnItem)

    returns.returnItems ++ aggregationItems ++ sortItems
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe = item match {
    case NodeByIndex(varName, idxName, key, value) =>
      new StartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: Iterable[Node] = graph.index.forNodes(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case NodeByIndexQuery(varName, idxName, query) =>
      new StartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: Iterable[Node] = graph.index.forNodes(idxName).query(queryText)
        indexHits.asScala
      })

    case NodeById(varName, id) => new StartPipe(lastPipe, varName, m => makeLongSeq(id(m), varName).map(graph.getNodeById))
    case RelationshipById(varName, ids@_*) => new StartPipe(lastPipe, varName, ids.map(graph.getRelationshipById))
  }


  def checkScalaVersion() {
    if (util.Properties.versionString.matches("^version 2.9.0")) {
      throw new Error("Cypher can only run with Scala 2.9.0. It looks like the Scala version is: " +
        util.Properties.versionString)
    }
  }

  private def makeLongSeq(result: Any, name: String): Seq[Long] = {
    if (result.isInstanceOf[Int]) {
      return Seq(result.asInstanceOf[Int].toLong)
    }

    if (result.isInstanceOf[Long]) {
      return Seq(result.asInstanceOf[Long])
    }

    def makeLong(x: Any): Long = x match {
      case i: Int => i.toLong
      case i: Long => i
      case i: String => i.toLong
    }

    if (result.isInstanceOf[java.lang.Iterable[_]]) {
      return result.asInstanceOf[java.lang.Iterable[_]].asScala.map(makeLong).toSeq
    }

    if (result.isInstanceOf[Traversable[_]]) {
      return result.asInstanceOf[Traversable[_]].map(makeLong).toSeq
    }

    throw new ParameterNotFoundException("Expected " + name + " to be a Long, or an Iterable of Long. It was '" + result + "'")
  }
}