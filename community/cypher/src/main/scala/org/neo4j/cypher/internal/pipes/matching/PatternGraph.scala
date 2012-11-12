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

import org.neo4j.cypher.internal.symbols.{MapType, SymbolTable}
import collection.mutable.{Set => MutableSet}
import org.neo4j.cypher.{PatternException, SyntaxException}

class PatternGraph(val patternNodes: Map[String, PatternNode],
                   val patternRels: Map[String, PatternRelationship],
                   val bindings: SymbolTable) {

  val (patternGraph, optionalElements, containsLoops, doubleOptionalPaths) = validatePattern(patternNodes, patternRels, bindings)

  def apply(key: String) = patternGraph(key)
  
  val hasDoubleOptionals:Boolean = doubleOptionalPaths.nonEmpty

  def get(key: String) = patternGraph.get(key)

  def contains(key: String) = patternGraph.contains(key)

  def keySet = patternGraph.keySet

  def containsOptionalElements = optionalElements.nonEmpty

  def boundElements = bindings.identifiers.filter(id => MapType().isAssignableFrom(id.typ)).map(_.name)

  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, PatternRelationship],
                              bindings: SymbolTable): (Map[String, PatternElement], Set[String], Boolean, Seq[DoubleOptionalPath]) = {
    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if (overlaps.nonEmpty) {
      throw new PatternException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, PatternElement] = (patternNodes.values ++ patternRels.values).map(x => (x.key -> x)).toMap
    val allElements = elementsMap.values.toSeq

    val boundElements = bindings.identifiers.flatMap(i => elementsMap.get(i.name))

    val hasLoops = checkIfWeHaveLoops(boundElements, allElements)
    val optionalSet = getOptionalElements(boundElements, allElements)
    val doubleOptionals = getDoubleOptionals(boundElements)

    (elementsMap, optionalSet, hasLoops, doubleOptionals)
  }

  private def getDoubleOptionals(boundPatternElements: Seq[PatternElement]): Seq[DoubleOptionalPath] = {
    var visited = Set[PatternElement]()
    var doubleOptionals = Seq[DoubleOptionalPath]()
    

    boundPatternElements.foreach(e => e.traverse(
      shouldFollow = e => !visited.contains(e),
      visit = (e, data: Seq[PatternElement]) => {
        visited += e
        val result = data :+ e

        val foundPathBetweenBoundElements = boundPatternElements.contains(e) && result.size > 1

        if (foundPathBetweenBoundElements) {
          val numberOfOptionals = result.foldLeft(0)((count, element) => {
            val r = element match {
              case x: PatternRelationship => if (x.optional) 1 else 0
              case _ => 0
            }

            count + r
          })

          if (numberOfOptionals > 2) {
            throw new PatternException("Your pattern has at least one path between two bound elements, and these patterns are undefined for the time being. Valid use cases for this are very interesting to us - let us know at cypher@neo4j.org")
          }
          
          if(numberOfOptionals > 1) {
            
            val leftNode = data(0)
            val leftRel = data(1)
            val rightNode = e
            val rightRel = data.last


            doubleOptionals = doubleOptionals ++ Seq[DoubleOptionalPath](
              DoubleOptionalPath(leftNode.key, rightNode.key, leftRel.key),
              DoubleOptionalPath(rightNode.key, leftNode.key, rightRel.key))
          }
        }


        result
      },
      data = Seq[PatternElement]()
    ))

    doubleOptionals
  }

  private def getOptionalElements(boundPatternElements: Seq[PatternElement], allPatternElements: Seq[PatternElement]): Set[String] = {
    val optionalElements = MutableSet[String](allPatternElements.map(_.key): _*)
    var visited = Set[PatternElement]()

    boundPatternElements.foreach(n => n.traverse(
      shouldFollow = e => {
        e match {
          case x: PatternNode => !visited.contains(e)
          case x: PatternRelationship => !visited.contains(e) && !x.optional
        }
      },
      visit = (e, x: Unit) => {
        optionalElements.remove(e.key)
        visited = visited ++ Set(e)
      },
      data = ()
    ))

    optionalElements.toSet
  }

  private def checkIfWeHaveLoops(boundPatternElements: Seq[PatternElement], allPatternElements: Seq[PatternElement]) = {
    var visited = Seq[PatternElement]()
    var loop = false

    val follow = (element: PatternElement) => element match {
      case n: PatternNode => true
      case r: PatternRelationship => !visited.contains(r)
    }

    val vNode = (n: PatternNode, x: Unit) => {
      if (visited.contains(n))
        loop = true
      visited = visited :+ n
    }

    val vRel = (r: PatternRelationship, x: Unit) => visited :+= r

    boundPatternElements.foreach {
      case pr: PatternRelationship => pr.startNode.traverse(follow, vNode, vRel, ())
      case pn: PatternNode => pn.traverse(follow, vNode, vRel, ())
    }

    val notVisitedElements = allPatternElements.filterNot(visited contains)
    if (notVisitedElements.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + notVisitedElements.map(_.key).mkString("", ", ", ""))
    }

    loop
  }
}
