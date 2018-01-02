/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3.commands.Pattern
import org.neo4j.cypher.internal.frontend.v2_3.PatternException

case class PatternGraph(patternNodes: Map[String, PatternNode],
                        patternRels: Map[String, Seq[PatternRelationship]],
                        boundElements: Seq[String],
                        patternsContained: Seq[Pattern]) {

  def nonEmpty: Boolean = !isEmpty

  def identifiers: Seq[String] = patternGraph.keys.toSeq

  def isEmpty: Boolean = patternNodes.isEmpty && patternRels.isEmpty

  val (patternGraph, containsLoops) = validatePattern(patternNodes, patternRels)

  lazy val hasBoundRelationships: Boolean = boundElements.exists(patternRels.keys.toSeq.contains)
  lazy val hasVarLengthPaths: Boolean = patternRels.values.exists(_.isInstanceOf[VariableLengthPatternRelationship])

  def apply(key: String) = patternGraph(key)

  def get(key: String) = patternGraph.get(key)

  def contains(key: String) = patternGraph.contains(key)

  def keySet = patternGraph.keySet

  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, Seq[PatternRelationship]]):
  (Map[String, Seq[PatternElement]], Boolean) = {

    if (isEmpty)
      return (Map(), false)

    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if (overlaps.nonEmpty) {
      throw new PatternException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, Seq[PatternElement]] = (patternNodes.values.map(Seq[PatternElement](_)) ++
      patternRels.values.asInstanceOf[Iterable[Seq[PatternElement]]]).map(x => x.head.key -> x).toMap
    val allElements = elementsMap.values.flatMap(_.toSeq).toSeq

    val boundPattern: Seq[PatternElement] = boundElements.flatMap(i => elementsMap.get(i)).flatMap(_.toSeq)

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
      patternRels.flatMap(tuple => {
        val patternRels = tuple._2
        patternRels.map(r => "(%s)-['%s']-(%s)".format(r.startNode.key, r, r.endNode.key))
      }).mkString(",")
  }
}

case class Relationships(closestRel: String, oppositeRel: String)
