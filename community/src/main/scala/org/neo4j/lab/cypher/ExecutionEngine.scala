/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.lab.cypher

import commands._
import pipes.{Pipe, FromPump}
import scala.collection.JavaConverters._
import org.neo4j.graphmatching.PatternRelationship
import org.neo4j.graphdb._
import org.neo4j.lab.cypher.filters._

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 09:44
 */
class ExecutionEngine(val graph: GraphDatabaseService) {
  type MapTransformer = (Map[String, Any]) => Map[String, Any]

  def createFilters(where: Option[Clause], patternKeeper: PatternKeeper): Filter = {

    def createFilter(clause: Clause): Filter = clause match {
      case And(a, b) => new AndFilter(createFilter(a), createFilter(b))
      case Or(a, b) => new OrFilter(createFilter(a), createFilter(b))
      case StringEquals(variable, property, value) => new EqualsFilter(variable, property, value)
    }

    where match {
      case None => new TrueFilter()
      case Some(clause) => createFilter(clause)
    }

    //    where match {
    //      case Some(w) => w.clauses.foreach((c) => {
    //        c match {
    //          case StringEquals(variable, propName, value) => {
    //            val patternPart = patternKeeper.getOrThrow(variable)
    //            patternPart.addPropertyConstraint(propName, CommonValueMatchers.exact(value))
    //          }
    //        }
    //      })
    //      case None =>
    //    }
  }

  def execute(query: Query): Projection = query match {
    case Query(select, start, matching, where, aggregation) => {
      val patternKeeper = new PatternKeeper
      val sourcePump: Pipe = createSourcePumps(start).reduceLeft(_ ++ _)

      addStartItemVariables(start, patternKeeper)

      val projections = createProjectionTransformers(select)

      createPattern(matching, patternKeeper)

      val filter = createFilters(where, patternKeeper)

      new Projection(patternKeeper.nodesMap, patternKeeper.relationshipsMap, sourcePump, projections, filter)
    }
  }

  def addStartItemVariables(start: Start, patternKeeper: PatternKeeper) {
    start.startItems.foreach((item) => {
      item match {
        case relItem: RelationshipStartItem => patternKeeper.getOrCreateRelationship(item.placeholderName)
        case nodeItem: NodeStartItem => patternKeeper.getOrCreateNode(item.placeholderName)
      }
      patternKeeper.getOrCreateNode(item.placeholderName)
    })
  }

  def createPattern(matching: Option[Match], patternKeeper: PatternKeeper) {
    matching match {
      case Some(m) => m.patterns.foreach((p) => {
        p match {
          case RelatedTo(left, right, relName, relationType, direction) => {
            val leftPattern = patternKeeper.getOrCreateNode(left)
            val rightPattern = patternKeeper.getOrCreateNode(right)
            val rel: PatternRelationship = relationType match {
              case Some(relType) => leftPattern.createRelationshipTo(rightPattern, DynamicRelationshipType.withName(relType), direction)
              case None => leftPattern.createRelationshipTo(rightPattern, direction)
            }


            relName match {
              case None =>
              case Some(name) => patternKeeper.addRelationship(name, rel)
            }
          }
        }
      })
      case None =>
    }
  }

  def createProjectionTransformers(select: Return): Seq[MapTransformer] = select.returnItems.map((selectItem) => {
    selectItem match {
      case PropertyOutput(nodeName, propName) => nodePropertyOutput(nodeName, propName) _
      case EntityOutput(nodeName) => nodeOutput(nodeName) _
    }
  })

  def nodeOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[Node]
    Map(column + "." + propName -> node.getProperty(propName))
  }

  private def createSourcePumps(from: Start): Seq[Pipe] = from.startItems.map(_ match {
    case NodeByIndex(varName, idxName, key, value) => {
      val indexHits: java.lang.Iterable[Node] = graph.index.forNodes(idxName).get(key, value)
      new FromPump(varName, indexHits.asScala)
    }
    case NodeById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getNodeById))
    case RelationshipById(varName, ids@_*) => new FromPump(varName, ids.map(graph.getRelationshipById))
  })
}