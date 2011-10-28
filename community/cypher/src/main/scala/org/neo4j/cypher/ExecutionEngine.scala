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
import parser.CypherParser
import pipes._
import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import collection.Seq
import java.lang.{Error, Iterable}
import java.util.{Map => JavaMap}


class ExecutionEngine(graph: GraphDatabaseService) {
  checkScalaVersion()

  require(graph != null, "Can't work with a null graph database")

  val parser = new CypherParser()

  @throws(classOf[SyntaxException])
  def execute(query: String): ExecutionResult = execute(parser.parse(query))

  @throws(classOf[SyntaxException])
  def execute(query: String, params: Map[String, Any]): ExecutionResult = {
    execute(parser.parse(query), params)
  }

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any]): ExecutionResult = {
    execute(parser.parse(query), params.asScala.toMap)
  }

  @throws(classOf[SyntaxException])
  def execute(query: Query): ExecutionResult = execute(query, Map[String, Any]())

  // This is here to support Java people
  @throws(classOf[SyntaxException])
  def execute(query: Query, map: JavaMap[String, Any]): ExecutionResult = execute(query, map.asScala.toMap)

  @throws(classOf[SyntaxException])
  def execute(query: Query, params: Map[String, Any]): ExecutionResult = query match {
    case Query(returns, start, matching, where, aggregation, sort, slice, namedPaths) => {

      val clauses = where match {
        case None => Seq()
        case Some(w) => w.atoms
      }

      val paramPipe = new ParameterPipe(params)
      val pipe = createSourcePumps(paramPipe, start.startItems.toList)

      var context = new CurrentContext(pipe, clauses)
      context = addFilters(context)

      context = createMatchPipe(matching, namedPaths, context)

      context.pipe = createShortestPathPipe(context.pipe, matching, namedPaths)
      context = addFilters(context)

      namedPaths match {
        case None =>
        case Some(x) => x.paths.foreach(p => context.pipe = new NamedPathPipe(context.pipe, p))
      }

      if (context.clauses.nonEmpty) {
        context.pipe = new FilterPipe(context.pipe, context.clauses.reduceLeft(_ ++ _))
      }

      val allReturnItems = extractReturnItems(returns, aggregation, sort)

      context.pipe = new TransformPipe(context.pipe, allReturnItems)

      aggregation match {
        case None =>
        case Some(aggr) => {
          context.pipe = new EagerAggregationPipe(context.pipe, returns.returnItems, aggr.aggregationItems)
        }
      }

      sort match {
        case None =>
        case Some(s) => {
          context.pipe = new SortPipe(context.pipe, s.sortItems.toList)
        }
      }

      slice match {
        case None =>
        case Some(x) => context.pipe = new SlicePipe(context.pipe, x.from, x.limit)
      }

      val returnItems = returns.returnItems ++ aggregation.getOrElse(new Aggregation()).aggregationItems

      val result = new ColumnFilterPipe(context.pipe, returnItems, returns.columns)

      result
    }
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
    shortestPaths.foreach(p => result = new ShortestPathPipe(result, p))
    result

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
      case x => context.pipe = new MatchPipe(context.pipe, x, context.clauses)
    }

    context
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

    case NodeById(varName, valueGenerator) => new NodeStartPipe(lastPipe, varName, m => makeNodes[Node](valueGenerator(m), varName, graph.getNodeById))
    case RelationshipById(varName, id) => new RelationshipStartPipe(lastPipe, varName, m => makeNodes[Relationship](id(m), varName, graph.getRelationshipById))
  }

  private def addFilters(context: CurrentContext): CurrentContext = {
    if (context.clauses.isEmpty) {
      context
    }
    else {
      val keys = context.pipe.symbols.identifiers.map(_.name)
      val matchingClauses = context.clauses.filter(x => {
        val unsatisfiedDependencies = x.dependsOn.filterNot(keys contains)
        unsatisfiedDependencies.isEmpty
      })
      if (matchingClauses.isEmpty) {
        context
      }
      else {
        val filterClause = matchingClauses.reduceLeft(_ ++ _)
        val p = new FilterPipe(context.pipe, filterClause)

        new CurrentContext(p, context.clauses.filterNot(matchingClauses contains))
      }
    }
  }

  def checkScalaVersion() {
    if (util.Properties.versionString.matches("^version 2.9.0")) {
      throw new Error("Cypher can only run with Scala 2.9.0. It looks like the Scala version is: " +
        util.Properties.versionString)
    }
  }

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
}

private class CurrentContext(var pipe: Pipe, var clauses: Seq[Clause])