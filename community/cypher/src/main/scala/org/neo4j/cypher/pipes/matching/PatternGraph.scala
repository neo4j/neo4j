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
package org.neo4j.cypher.pipes.matching

import org.neo4j.cypher.symbols.SymbolTable
import org.neo4j.cypher.SyntaxException

class PatternGraph(patternNodes: Map[String, PatternNode],
                   patternRels: Map[String, PatternRelationship],
                   bindings: SymbolTable) {

  val (patternGraph, optionalElements, hasLoops) = validatePattern(patternNodes, patternRels, bindings)

  def apply(key: String) = patternGraph(key)

  def get(key: String) = patternGraph.get(key)

  def contains(key: String) = patternGraph.contains(key)

  def keySet = patternGraph.keySet

  def containsOptionalElements = optionalElements.nonEmpty

  /*
  This method is mutable, but it is only called from the constructor of this class. The created pattern graph
   is immutable and thread safe.
  */
  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, PatternRelationship],
                              bindings: SymbolTable): (Map[String, PatternElement], Set[String], Boolean) = {
    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if (overlaps.nonEmpty) {
      throw new SyntaxException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, PatternElement] = (patternNodes.values ++ patternRels.values).map(x => (x.key -> x)).toMap
    val optionalElements = scala.collection.mutable.Set[String](elementsMap.keys.toSeq: _*)

    def markMandatoryElements(x: PatternElement) {
      if (optionalElements.contains(x.key)) {
        x match {
          case nod: PatternNode => {
            optionalElements.remove(x.key)
            nod.relationships.filter(!_.optional).foreach(markMandatoryElements)
          }
          case rel: PatternRelationship => if (!rel.optional) {
            optionalElements.remove(x.key)
            markMandatoryElements(rel.startNode)
            markMandatoryElements(rel.endNode)
          }
        }
      }
    }

    var visited = scala.collection.mutable.Seq[PatternElement]()
    var hasLoops = false

    def visit(x: PatternElement) {
      if (!visited.contains(x)) {
        visited = visited ++ Seq(x)
        x match {
          case nod: PatternNode => nod.relationships.filterNot(visited.contains).foreach(rel => {
            visited = visited ++ Seq(rel)
            visit(rel.getOtherNode(nod))
          })
          case rel: PatternRelationship => {
            visit(rel.startNode)
            visit(rel.endNode)
          }
        }
      } else {
        hasLoops = true
      }
    }

    bindings.identifiers.foreach(id => {
      val el = elementsMap.get(id.name)
      el match {
        case None =>
        case Some(x) => {
          if (!visited.contains(x)) visit(x)
          markMandatoryElements(x)
        }
      }
    })

    val notVisited = elementsMap.values.filterNot(visited contains)

    if (notVisited.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + notVisited.map(_.key).mkString("", ", ", ""))
    }

    (elementsMap, optionalElements.toSet, hasLoops)
  }
}
