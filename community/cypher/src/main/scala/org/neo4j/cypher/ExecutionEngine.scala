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

class ExecutionEngine(graph: GraphDatabaseService) {
  checkScalaVersion()


  def extractReturnItems(returns: Return, aggregation: Option[Aggregation], sort: Option[Sort]): Seq[ReturnItem] = {
    val aggregation1 = aggregation.getOrElse(new Aggregation())
    val sort1 = sort.getOrElse(new Sort())

    val aggregationItems = aggregation1.aggregationItems.map(_.concreteReturnItem)
    val sortItems = sort1.sortItems.map(_.returnItem.concreteReturnItem)

    returns.returnItems ++ aggregationItems ++ sortItems
  }

  @throws(classOf[SyntaxException])
  def execute(query: Query): ExecutionResult = query match {
    case Query(returns, start, matching, where, aggregation, sort, slice, namedPaths) => {
      var pipe = createSourcePumps(start).reduceLeft(_ ++ _)

      pipe = createMatchPipe(matching, namedPaths, pipe)

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

  private def createSourcePumps(from: Start): Seq[Pipe] =
    from.startItems.map(item =>
      item match {
        case NodeByIndex(varName, idxName, key, value) =>
          new StartPipe(varName, () => {
            val indexHits: Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
            indexHits.asScala
          })

        case NodeByIndexQuery(varName, idxName, query) =>
          new StartPipe(varName, () => {
            val indexHits: Iterable[Node] = graph.index.forNodes(idxName).query(query)
            indexHits.asScala
          })

        case NodeById(varName, ids@_*) => new StartPipe(varName, ids.map(graph.getNodeById))
        case RelationshipById(varName, ids@_*) => new StartPipe(varName, ids.map(graph.getRelationshipById))

      })

  def checkScalaVersion() {
    if (util.Properties.versionString.matches("^version 2.9.0")) {
      throw new Error("Cypher can only run with Scala 2.9.0. It looks like the Scala version is: " +
        util.Properties.versionString)
    }
  }
}