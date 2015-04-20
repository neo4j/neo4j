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

import org.neo4j.cypher.internal.compiler.v2_2.PatternException
import org.neo4j.cypher.internal.compiler.v2_2.commands.Pattern

case class PatternGraph(patternNodes: Map[String, PatternNode],
                        patternRels: Map[String, PatternRelationship],
                        boundElements: Seq[String],
                        patternsContained: Seq[Pattern]) {

  def nonEmpty: Boolean = !isEmpty

  def identifiers: Seq[String] = patternGraph.keys.toSeq

  def isEmpty: Boolean = patternNodes.isEmpty && patternRels.isEmpty

  val (patternGraph, containsLoops) = validatePattern(patternNodes, patternRels)

  lazy val hasBoundRelationships: Boolean = boundElements.exists(patternRels.keys.toSeq.contains)
  lazy val hasVarLengthPaths: Boolean = patternRels.values.exists(_.isInstanceOf[VariableLengthPatternRelationship])

  def extractGraphFromPaths(relationshipsNotInDoubleOptionalPaths: Iterable[PatternRelationship], boundPoints: Seq[String]): PatternGraph = {
    val oldNodes = relationshipsNotInDoubleOptionalPaths.flatMap(p => Seq(p.startNode, p.endNode)).toSeq.distinct

    val newNodes = oldNodes.map(patternNode => patternNode.key ->
      new PatternNode(patternNode.key, patternNode.labels, patternNode.properties)).toMap

    val newRelationships = relationshipsNotInDoubleOptionalPaths.map {
      case pr: VariableLengthPatternRelationship => ???
      case pr: PatternRelationship               =>
        val s = newNodes(pr.startNode.key)
        val e = newNodes(pr.endNode.key)
        pr.key -> s.relateTo(pr.key, e, pr.relTypes, pr.dir)
    }.toMap

    new PatternGraph(newNodes, newRelationships, boundPoints, Seq.empty /* This is only used for plan building and is not needed here */)
  }

  def apply(key: String) = patternGraph(key)

  def get(key: String) = patternGraph.get(key)

  def contains(key: String) = patternGraph.contains(key)

  def keySet = patternGraph.keySet

  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, PatternRelationship]):
  (Map[String, PatternElement], Boolean) = {

    if (isEmpty)
      return (Map(), false)

    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if (overlaps.nonEmpty) {
      throw new PatternException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, PatternElement] = (patternNodes.values ++ patternRels.values).map(x => (x.key -> x)).toMap
    val allElements = elementsMap.values.toSeq

    val boundPattern: Seq[PatternElement] = boundElements.flatMap(i => elementsMap.get(i))

    val hasLoops = checkIfWeHaveLoops(boundPattern, allElements)

    (elementsMap, hasLoops)
  }

  private def checkIfWeHaveLoops(boundPatternElements: Seq[PatternElement], allPatternElements: Seq[PatternElement]) = {
    var visited = Seq[PatternElement]()
    var loop = false

    def follow(element: PatternElement) = element match {
      case n: PatternNode         => true
      case r: PatternRelationship => !visited.contains(r)
    }

    def visit_node(n: PatternNode, x: Unit) {
      if (visited.contains(n))
        loop = true
      visited = visited :+ n
    }

    def visit_relationship(r: PatternRelationship, x: Unit) {
      visited :+= r
    }

    boundPatternElements.foreach {
      case pr: PatternRelationship => pr.startNode.traverse(follow, visit_node, visit_relationship, (), Seq())
      case pn: PatternNode         => pn.traverse(follow, visit_node, visit_relationship, (), Seq())
    }

    loop
  }

  override def toString = if(patternRels.isEmpty && patternNodes.isEmpty) {
      "[EMPTY PATTERN]"
  } else {
      patternRels.map(tuple=> {
        val r = tuple._2
        "(%s)-['%s']-(%s)".format(r.startNode.key, r, r.endNode.key)
      }).mkString(",")
  }
}

case class Relationships(closestRel: String, oppositeRel: String)
