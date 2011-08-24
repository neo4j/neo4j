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
import index.IndexHits
import java.lang.{Error, Iterable}

class ExecutionEngine(graph: GraphDatabaseService) {
  checkScalaVersion()


  @throws(classOf[SyntaxException])
  def execute(query: Query): ExecutionResult = query match {
    case Query(returns, start, matching, where, aggregation, sort, slice) => {
      var pipe = createSourcePumps(start).reduceLeft(_ ++ _)

      matching match {
        case None =>
        case Some(m) => {
          val patterns = m.patterns.map(p => p match {
            case r: RelatedTo => List(r)
            case p: PathItem => p.pathPattern.toList
          }).flatten

          pipe = new PatternPipe(pipe, patterns)

          val paths = m.patterns.filter(_.isInstanceOf[PathItem])
          paths.foreach(p => pipe = new PathPipe(pipe, p.asInstanceOf[PathItem]))
        }
      }

      where match {
        case None =>
        case Some(w) => pipe = new FilterPipe(pipe, w)
      }

      val allReturnItems = returns.returnItems ++
        aggregation.getOrElse(new Aggregation()).aggregationItems.map(_.concreteReturnItem) ++
        sort.getOrElse(new Sort()).sortItems.map(_.returnItem.concreteReturnItem)

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

  private def createSourcePumps(from: Start): Seq[Pipe] =
    from.startItems.map((item) => {
      item match {
        case NodeByIndex(varName, idxName, key, value) => {
          new StartPipe(varName, () => {
            val indexHits: Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
            indexHits.asScala
          })
        }
        case NodeByIndexQuery(varName, idxName, query) => {
          new StartPipe(varName, ()=>{
            val indexHits: Iterable[Node] = graph.index.forNodes(idxName).query(query)
            indexHits.asScala
          })
        }
        case NodeById(varName, ids@_*) => new StartPipe(varName, ids.map(graph.getNodeById))
        case RelationshipById(varName, ids@_*) => new StartPipe(varName, ids.map(graph.getRelationshipById))
      }
    })

  def checkScalaVersion() {
    if (util.Properties.versionString.matches("^version 2.9.0")) {
      throw new Error("Cypher can only run with Scala 2.9.0. It looks like the Scala version is: " +
        util.Properties.versionString)
    }
  }
}