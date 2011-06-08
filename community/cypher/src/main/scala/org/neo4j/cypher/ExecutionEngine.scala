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
import pipes.{Pipe, FromPump}
import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import org.neo4j.graphmatching.{PatternRelationship, PatternNode, AbstractPatternObject}
import java.lang.Iterable
import collection.Seq


class ExecutionEngine(val graph: GraphDatabaseService)
{
  type MapTransformer = Map[String, Any] => Map[String, Any]

  def createSortItems(sort: Option[Sort], patternKeeper: SymbolTable): Seq[MapTransformer] =
  {
    sort match
    {
      case None => Seq[MapTransformer]()
      case Some(x) => createMapTransformers(x.sortItems, patternKeeper)
    }
  }

  @throws(classOf[SyntaxError])
  def execute(query: Query): ExecutionResult = query match
  {
    case Query(returns, start, matching, where, aggregation, sort) =>
    {
      val patternKeeper = new SymbolTable
      val sourcePump: Pipe = createSourcePumps(start).reduceLeft(_ ++ _)

      patternKeeper.addStartItems(start)

      createPattern(matching, patternKeeper)

        patternKeeper.checkConnectednessOfPatternGraph(start)

      val filter = createFilters(where, patternKeeper)

      val projections = createMapTransformers(returns.returnItems, patternKeeper)
      val columns:List[String] = returns.returnItems.map(_.identifier).toList
      //      val sortItems = createSortItems(sort, patternKeeper)

      new Projection(patternKeeper.nodesMap, patternKeeper.relationshipsMap, sourcePump, projections, filter, columns)
    }
  }

  def createFilters(where: Option[Clause], patternKeeper: SymbolTable): Clause =
  {
    where match
    {
      case None => new True()
      case Some(clause) => clause
    }
  }

  def createPattern(matching: Option[Match], patternKeeper: SymbolTable)
  {
    matching match
    {
      case Some(m) => m.patterns.foreach((pattern) =>
      {
        pattern match
        {
          case RelatedTo(left, right, relName, relationType, direction) => createRelationshipPattern(patternKeeper, left, right, relationType, direction, relName)
        }
      })
      case None =>
    }
  }

  def createRelationshipPattern(patternKeeper: SymbolTable, left: String, right: String, relationType: Option[String], direction: Direction, relName: Option[String])
  {
    val leftPattern = patternKeeper.getOrCreateNode(left)
    val rightPattern = patternKeeper.getOrCreateNode(right)
    val rel = relationType match
    {
      case Some(relType) => leftPattern.createRelationshipTo(rightPattern, DynamicRelationshipType.withName(relType), direction)
      case None => leftPattern.createRelationshipTo(rightPattern, direction)
    }

    relName match
    {
      case None =>
      case Some(name) =>
      {
        patternKeeper.addRelationship(name, rel)
        rel.setLabel(name)
      }
    }
  }

  def createMapTransformers(returnItems: Seq[ReturnItem], patternKeeper: SymbolTable): Seq[MapTransformer] =
  {

    returnItems.map((selectItem) =>
    {
      selectItem match
      {
        case PropertyOutput(nodeName, propName) =>
        {
          patternKeeper.assertHas(nodeName)
          nodePropertyOutput(nodeName, propName) _
        }

        case NullablePropertyOutput(nodeName, propName) =>
        {
          patternKeeper.assertHas(nodeName)
          nullableNodePropertyOutput(nodeName, propName) _
        }

        case EntityOutput(nodeName) =>
        {
          patternKeeper.assertHas(nodeName)
          nodeOutput(nodeName) _
        }
      }
    })
  }

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] =
  {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[PropertyContainer]
    Map(column + "." + propName -> node.getProperty(propName))
  }

  def nullableNodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] =
  {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[PropertyContainer]

    val property = try
    {
      node.getProperty(propName)
    } catch
    {
      case x: NotFoundException => null
    }

    Map(column + "." + propName -> property)
  }

  private def createSourcePumps(from: Start): Seq[Pipe] =
    from.startItems.map((item) =>
    {
      item match
      {
        case NodeByIndex(varName, idxName, key, value) =>
        {
          val indexHits: Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
          new FromPump(varName, indexHits.asScala.toList)
        }
        case NodeById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getNodeById))
        case RelationshipById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getRelationshipById))
      }
    })

}