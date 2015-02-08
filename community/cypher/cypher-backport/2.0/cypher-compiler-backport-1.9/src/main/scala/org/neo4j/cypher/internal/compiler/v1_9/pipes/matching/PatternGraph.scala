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

import collection.mutable.{Set => MutableSet}
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.PatternException
import org.neo4j.cypher.internal.compiler.v1_9.commands.Pattern

case class PatternGraph(patternNodes: Map[String, PatternNode],
                        patternRels: Map[String, PatternRelationship],
                        boundElements: Seq[String],
                        patternsContained: Seq[Pattern]) {
  def nonEmpty: Boolean = !isEmpty


  def isEmpty: Boolean = patternNodes.isEmpty && patternRels.isEmpty

  val (patternGraph, optionalElements, containsLoops, doubleOptionalPaths) = validatePattern(patternNodes, patternRels)

  def doubleOptionalPatterns(): Seq[PatternGraph] = {
    val newBoundElements = ((mandatoryGraph.patternNodes.keys ++ mandatoryGraph.patternRels.keys).toSeq ++ boundElements).distinct

    doubleOptionalPaths.map(
      dop => {
        val relationships = Seq(patternRels(dop.rel1), patternRels(dop.rel2))
        extractGraphFromPaths(relationships, newBoundElements)
      }
    )
  }

  lazy val hasBoundRelationships: Boolean = boundElements.exists(patternRels.keys.toSeq.contains)
  lazy val hasVarLengthPaths: Boolean = patternRels.values.exists(_.isInstanceOf[VariableLengthPatternRelationship])

  lazy val mandatoryGraph: PatternGraph = {
    val relationshipsNotInDoubleOptionalPaths = patternRels.values.filterNot(p => doubleOptionalPaths.exists(dop => dop.path.exists(_.key == p.key)))

    if (relationshipsNotInDoubleOptionalPaths.size == patternRels.size)
      this
    else
      extractGraphFromPaths(relationshipsNotInDoubleOptionalPaths, boundElements)
  }


  def extractGraphFromPaths(relationshipsNotInDoubleOptionalPaths: Iterable[PatternRelationship], boundPoints: Seq[String]): PatternGraph = {
    val oldNodes = relationshipsNotInDoubleOptionalPaths.flatMap(p => Seq(p.startNode, p.endNode)).toSeq.distinct

    val newNodes = oldNodes.map(patternNode => patternNode.key -> new PatternNode(patternNode.key)).toMap
    val newRelationships = relationshipsNotInDoubleOptionalPaths.map {
      case pr: VariableLengthPatternRelationship => ???
      case pr: PatternRelationship               =>
        val s = newNodes(pr.startNode.key)
        val e = newNodes(pr.endNode.key)
        pr.key -> s.relateTo(pr.key, e, pr.relTypes, pr.dir, pr.optional, pr.predicate)
    }.toMap

    new PatternGraph(newNodes, newRelationships, boundPoints, Seq.empty /* This is only used for plan building and is not needed here */)
  }

  def apply(key: String) = patternGraph(key)

  val hasDoubleOptionals: Boolean = doubleOptionalPaths.nonEmpty

  def get(key: String) = patternGraph.get(key)

  def contains(key: String) = patternGraph.contains(key)

  def keySet = patternGraph.keySet

  lazy val containsOptionalElements = optionalElements.nonEmpty

  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, PatternRelationship]):
  (Map[String, PatternElement], Set[String], Boolean, Seq[DoubleOptionalPath]) = {

    if (isEmpty)
      return (Map(), Set(), false, Seq())

    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if (overlaps.nonEmpty) {
      throw new PatternException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, PatternElement] = (patternNodes.values ++ patternRels.values).map(x => (x.key -> x)).toMap
    val allElements = elementsMap.values.toSeq

    val boundPattern: Seq[PatternElement] = boundElements.flatMap(i => elementsMap.get(i))

    val hasLoops = checkIfWeHaveLoops(boundPattern, allElements)
    val optionalSet = getOptionalElements(boundPattern, allElements)
    val doubleOptionals = getDoubleOptionals(boundPattern, optionalSet)

    (elementsMap, optionalSet, hasLoops, doubleOptionals)
  }

  private def getDoubleOptionals(boundPatternElements: Seq[PatternElement], optionalElements: Set[String]): Seq[DoubleOptionalPath] = {
    var doubleOptionals = Seq[DoubleOptionalPath]()


    boundPatternElements.foreach(e => e.traverse(
      shouldFollow = e => true,
      visit = (e, data: Seq[PatternElement]) => {
        val result = data :+ e

        val foundPathBetweenBoundElements = boundPatternElements.contains(e) && result.size > 1

        if (foundPathBetweenBoundElements) {
          val init = Seq[PatternRelationship]()

          val numberOfOptionals = result.foldLeft(init)((count, element) => {
            val r = element match {
              case x: PatternRelationship if x.optional => Some(x)
              case _                                    => None
            }

            count ++ r
          })

          if (numberOfOptionals.size > 2) {
            throw new PatternException("Your pattern has at least one path between two bound elements with more than two optional relationships. This is currently not supported by Cypher")
          }

          if (numberOfOptionals.size == 2) {
            val leftRel = numberOfOptionals(0)
            val rightRel = numberOfOptionals(1)
            val leftNode = data(data.indexOf(leftRel) - 1)
            val leftIdx = data.indexOf(leftNode)
            val rightIdx = data.indexOf(rightRel) + 2

            val path: Seq[PatternElement] = result.slice(leftIdx, rightIdx)

            val correctSidedPath = if (path.head.key < path.last.key) {
              path
            } else
              path.reverse

            doubleOptionals = doubleOptionals :+ DoubleOptionalPath(correctSidedPath)
          }
        }


        result
      },
      data = Seq[PatternElement](),
      path = Seq()
    ))

    //Before we return, let's remove all double optional paths that have a at least one node between the optional
    //relationships that is not optional.
    val filteredDoubleOptionalPaths = doubleOptionals.distinct.filterNot(dop => {
      val optionalPartOfDop: Seq[PatternElement] = dop.path.tail.reverse.tail
      optionalPartOfDop.exists(x => !optionalElements.contains(x.key))
    })

    checkForUnsupportedPatterns(filteredDoubleOptionalPaths)

    filteredDoubleOptionalPaths
  }

  private def checkForUnsupportedPatterns(paths: Seq[DoubleOptionalPath]) {
    paths.foreach {
      case dop =>
        val otherDoubleOptionalPaths = paths.filterNot(_ == dop)
        val elementsInOtherPaths = otherDoubleOptionalPaths.flatMap(p => p.path.slice(1,p.path.size-2)).distinct

        val sharedPatterns = dop.path.filter(elementsInOtherPaths.contains)
        if (sharedPatterns.nonEmpty) {
          throw new PatternException("This pattern is not supported right now. These pattern elements are part of " +
                                     "multiple double optional paths, and that is not allowed. " + sharedPatterns.map(_.key).mkString(","))
        }
    }
  }

  private def getOptionalElements(boundPatternElements: Seq[PatternElement], allPatternElements: Seq[PatternElement]): Set[String] = {
    val optionalElements = MutableSet[String](allPatternElements.map(_.key): _*)
    var visited = Set[PatternElement]()

    boundPatternElements.foreach(n => n.traverse(
      shouldFollow = e => {
        e match {
          case x: PatternNode         => !visited.contains(e)
          case x: PatternRelationship => !visited.contains(e) && !x.optional
        }
      },
      visit = (e, x: Unit) => {
        optionalElements.remove(e.key)
        visited = visited ++ Set(e)
      },
      data = (),
      path = Seq()
    ))

    optionalElements.toSet
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

    val notVisitedElements = allPatternElements.filterNot(visited contains)
    if (notVisitedElements.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + notVisitedElements.map(_.key).sorted.mkString(", "))
    }

    loop
  }

  override def toString = patternRels.map(tuple=> {
    val r = tuple._2
    "(%s)-['%s']-(%s)".format(r.startNode.key, r, r.endNode.key)
  }).mkString(",")
}

case class Relationships(closestRel: String, oppositeRel: String)

case class DoubleOptionalPath(path: Seq[PatternElement]) {

  assert(isProperPath, "The DoubleOptionalPath created is not valid: " + path.mkString(","))

  def isProperPath: Boolean = {
    var x = true
    val (nodes, rels) = path.partition(e => {
      x = !x
      !x
    })

    val nodesContainOnlyNodes = nodes.forall(_.isInstanceOf[PatternNode])
    val relsAreAllRels = rels.forall(_.isInstanceOf[PatternRelationship])
    val atLeastOneNode = nodes.length > 0
    val relsLengthEqualsToNodesLengthMinusOne = rels.length == nodes.length - 1
    nodesContainOnlyNodes && relsAreAllRels && atLeastOneNode && relsLengthEqualsToNodesLengthMinusOne
  }

  val startNode = path.head.key
  val endNode = path.last.key
  val rel1 = path.tail.head.key
  val rel2 = path.reverse.tail.head.key

  def canRun(s: String): Boolean = startNode == s || endNode == s

  def otherNode(nodeName: String) = nodeName match {
    case x if x == startNode => endNode
    case x if x == endNode   => startNode
  }

  def relationshipsSeenFrom(nodeName: String) = nodeName match {
    case x if x == startNode => Relationships(rel1, rel2)
    case x if x == endNode   => Relationships(rel2, rel1)
  }

  def thisRel(nodeName: String) = nodeName match {
    case x if x == startNode => rel1
    case x if x == endNode   => rel2
  }

  def shouldDoWork(current: String, remaining: Set[MatchingPair]): Boolean = {
    val fromLeft = startNode == current && remaining.exists(_.patternNode.key == endNode)
    val fromRight = endNode == current && remaining.exists(_.patternNode.key == startNode)
    fromLeft || fromRight
  }
}
