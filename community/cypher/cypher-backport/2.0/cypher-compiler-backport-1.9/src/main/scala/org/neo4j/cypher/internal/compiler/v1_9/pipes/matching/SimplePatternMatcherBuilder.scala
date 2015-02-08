/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes.matching

import collection.{immutable, Map}
import org.neo4j.graphdb.{Relationship, Node, DynamicRelationshipType}
import org.neo4j.graphmatching.{PatternMatcher => SimplePatternMatcher, PatternNode => SimplePatternNode, PatternRelationship=>SimplePatternRelationship}
import collection.JavaConverters._
import org.neo4j.cypher.internal.compiler.v1_9.commands.{Predicate, True}
import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState

class SimplePatternMatcherBuilder(pattern: PatternGraph, predicates: Seq[Predicate], symbolTable: SymbolTable) extends MatcherBuilder {
  def createPatternNodes: immutable.Map[String, SimplePatternNode] = {
    pattern.patternNodes.map {
      case (key, pn) => {
        key -> {
          new SimplePatternNode(pn.key)
        }
      }
    }
  }

  def createPatternRels(patternNodes:immutable.Map[String, SimplePatternNode]):immutable.Map[String, SimplePatternRelationship]  = pattern.patternRels.map {
    case (key, pr) => {
      val start = patternNodes(pr.startNode.key)
      val end = patternNodes(pr.endNode.key)

      val patternRel = if (pr.relTypes.isEmpty)
        start.createRelationshipTo(end, pr.dir)
      else {
        // The SimplePatternMatcher does not support multiple relationship types
        // re-visit this if it ever does
        start.createRelationshipTo(end, DynamicRelationshipType.withName(pr.relTypes.head), pr.dir)
      }

      patternRel.setLabel(pr.key)

      key -> patternRel
    }
  }

  def setAssociations(sourceRow: Map[String, Any]): (immutable.Map[String, SimplePatternNode], immutable.Map[String, SimplePatternRelationship]) = {
    val patternNodes = createPatternNodes
    val patternRels = createPatternRels(patternNodes)
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

    (patternNodes, patternRels)
  }

  def getMatches(ctx: ExecutionContext, state:QueryState) = {
    val (patternNodes, patternRels) = setAssociations(ctx)
    val validPredicates = predicates.filter(p => p.symbolDependenciesMet(symbolTable))
    val startPoint = patternNodes.values.find(_.getAssociation != null).get
    SimplePatternMatcher.getMatcher.`match`(startPoint, startPoint.getAssociation).asScala.flatMap(patternMatch => {
      val result = ctx.clone

      patternNodes.foreach {
        case (key, pn) => result += key -> patternMatch.getNodeFor(pn)
      }
      patternRels.foreach {
        case (key, pr) => result += key -> patternMatch.getRelationshipFor(pr)
      }

      Some(result).filter(r => validPredicates.forall(_.isMatch(r)(state)))
    })
  }
}

object SimplePatternMatcherBuilder {
  def canHandle(graph: PatternGraph): Boolean = {
    val a = !graph.containsOptionalElements
    val b = !graph.patternRels.values.exists(pr => pr.isInstanceOf[VariableLengthPatternRelationship] || pr.predicate != True() || pr.startNode == pr.endNode || pr.relTypes.size > 1)
    val c = !graph.patternRels.keys.exists(graph.boundElements.contains)
    val d = !graph.patternNodes.values.exists(pn => pn.relationships.isEmpty)
    a && b && c && d
  }
}
