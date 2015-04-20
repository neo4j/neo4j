/*
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_2._
import commands.Predicate
import pipes.QueryState
import symbols._
import org.neo4j.graphdb.{Relationship, Node, DynamicRelationshipType}
import org.neo4j.graphmatching.{PatternMatcher => SimplePatternMatcher, PatternNode => SimplePatternNode,
PatternRelationship => SimplePatternRelationship, PatternMatch}
import collection.{immutable, Map}
import collection.JavaConverters._

class SimplePatternMatcherBuilder(pattern: PatternGraph,
                                  predicates: Seq[Predicate],
                                  symbolTable: SymbolTable,
                                  identifiersInClause: Set[String]) extends MatcherBuilder {
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
        case _                => pn.setAssociation(null)
      }
    })

    patternRels.values.foreach(pr => {
      sourceRow.get(pr.getLabel) match {
        case Some(rel: Relationship) => pr.setAssociation(rel)
        case _                       => pr.setAssociation(null)
      }
    })

    (patternNodes, patternRels)
  }

  def getMatches(ctx: ExecutionContext, state: QueryState) = {
    val (patternNodes, patternRels) = setAssociations(ctx)
    val validPredicates = predicates.filter(p => p.symbolDependenciesMet(symbolTable))
    val startPoint = patternNodes.values.find(_.getAssociation != null).get

    val incomingRels: Set[Relationship] = ctx.collect {
      case (k, r: Relationship) if identifiersInClause.contains(k) => r
    }.toSet

    val boundRels = patternRels.values.collect {
      case r: SimplePatternRelationship if r.getAssociation != null => r.getAssociation
    }.toSet
    val unboundIncomingRels = incomingRels -- boundRels

    val alreadyUsed = { pattern: PatternMatch =>
      patternRels.values.exists { r =>
          val relationship = pattern.getRelationshipFor(r)
          unboundIncomingRels.contains(relationship)
      }
    }

    SimplePatternMatcher.getMatcher.`match`(startPoint, startPoint.getAssociation).asScala.flatMap(patternMatch => {
      if (alreadyUsed(patternMatch)) {
        None
      } else {
        val result: ExecutionContext = ctx.clone()

        patternNodes.foreach {
          case (key, pn) => val tuple = key -> patternMatch.getNodeFor(pn)
            result += tuple
        }
        patternRels.foreach {
          case (key, pr) => val tuple = key -> patternMatch.getRelationshipFor(pr)
            result += tuple
        }

        Some(result).filter(r => validPredicates.forall(_.isTrue(r)(state)))
      }
    })
  }

  def name = "SimplePatternMatcher"
}

object SimplePatternMatcherBuilder {
  def canHandle(graph: PatternGraph): Boolean = {
    val a = !graph.patternRels.values.exists(pr => pr.isInstanceOf[VariableLengthPatternRelationship] || pr.startNode == pr.endNode || pr.relTypes.size > 1)
    val b = !graph.patternRels.keys.exists(graph.boundElements.contains)
    val c = !graph.patternNodes.values.exists(pn => pn.relationships.isEmpty )
    val d = !graph.patternNodes.values.exists(node => node.labels.nonEmpty || node.properties.nonEmpty)
    val e = !graph.patternRels.values.exists(rel => rel.properties.nonEmpty)
    a && b && c && d && e
  }
}
