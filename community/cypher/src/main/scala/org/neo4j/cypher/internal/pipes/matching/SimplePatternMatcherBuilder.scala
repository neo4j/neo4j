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
package org.neo4j.cypher.internal.pipes.matching

import collection.Map
import org.neo4j.graphdb.{Relationship, Node, DynamicRelationshipType}
import org.neo4j.graphmatching.{PatternMatcher => SimplePatternMatcher, PatternNode => SimplePatternNode}
import collection.JavaConverters._
import org.neo4j.cypher.internal.commands.True

import collection.breakOut
class SimplePatternMatcherBuilder(pattern: PatternGraph) extends MatcherBuilder {
  val patternNodes = pattern.patternNodes.map {
    case (key, pn) => {
      key -> {
        new SimplePatternNode(pn.key)
      }
    }
  }

  val patternRels = pattern.patternRels.map {
    case (key, pr) => {
      val start = patternNodes(pr.startNode.key)
      val end = patternNodes(pr.endNode.key)

      val patternRel = pr.relType match {
        case None => start.createRelationshipTo(end, pr.dir)
        case Some(typ) => start.createRelationshipTo(end, DynamicRelationshipType.withName(typ), pr.dir)
      }

      patternRel.setLabel(pr.key)

      key->patternRel
    }
  }

  def setAssociations(sourceRow: Map[String, Any]) {
    patternNodes.values.foreach(pn => {
      sourceRow.get(pn.getLabel) match {
        case Some(node: Node) => pn.setAssociation(node)
        case _ => pn.setAssociation(null)
      }
    })

    patternRels.values.foreach(pr => {
      sourceRow.get(pr.getLabel) match {
        case Some(rel: Relationship) => pr.setAssociation(rel)
        case _ => pr.setAssociation(null)
      }
    })
  }

  def getMatches(sourceRow: Map[String, Any]) = {
    setAssociations(sourceRow)
    val result = collection.mutable.Map(sourceRow.toSeq:_*)

    val startPoint = patternNodes.values.find(_.getAssociation != null).get
    SimplePatternMatcher.getMatcher.`match`(startPoint, startPoint.getAssociation).asScala.map( patternMatch =>{
      patternNodes.foreach{case (key, pn) => result += key -> patternMatch.getNodeFor(pn)}
      patternRels.foreach{case (key, pr) => result += key -> patternMatch.getRelationshipFor(pr)}

      result.clone()
    })
  }
}

object SimplePatternMatcherBuilder {
  def canHandle(graph: PatternGraph): Boolean = {
    val a = !graph.containsOptionalElements
    val b = !graph.patternRels.values.exists(pr => pr.isInstanceOf[VariableLengthPatternRelationship] || pr.predicate != True() || pr.startNode == pr.endNode)
    val c = !graph.patternRels.keys.exists( graph.boundElements.contains )
    val d = !graph.patternNodes.values.exists( pn => pn.relationships.isEmpty)
    a && b && c && d
  }
}