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
package org.neo4j.cypher.pipes.matching

import org.neo4j.cypher.{SyntaxException, SymbolTable}
import org.neo4j.cypher.commands._
import collection.immutable.Map
import org.neo4j.graphdb.{PropertyContainer, Direction, Relationship, Node}
import collection.{Traversable, Seq, Iterable}

class MatchingContext(patterns: Seq[Pattern], boundIdentifiers: SymbolTable, clauses: Seq[Clause] = Seq()) {
  type PatternGraph = Map[String, PatternElement]

  val (patternGraph, optionalElements) = buildPatternGraph()

  def getMatches(sourceRow: Map[String, Any]): Traversable[Map[String, Any]] = {
    val bindings: Map[String, Any] = sourceRow.filter(_._2.isInstanceOf[PropertyContainer])
    val boundPairs: Map[String, MatchingPair] = extractBoundMatchingPairs(bindings)

    val undirectedBoundRelationships: Iterable[PatternRelationship] = bindings.keys.
      filter(patternGraph.contains(_)).
      filter(patternGraph(_).isInstanceOf[PatternRelationship]).
      map(patternGraph(_).asInstanceOf[PatternRelationship]).
      filter(_.dir == Direction.BOTH)

    val mandatoryPattern: Traversable[Map[String, Any]] = if (undirectedBoundRelationships.isEmpty) {
      createPatternMatcher(boundPairs, false)
    } else {
      val boundRels: Seq[Map[String, MatchingPair]] = createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships, bindings)

      boundRels.map(relMap => createPatternMatcher(relMap ++ boundPairs, false)).reduceLeft(_ ++ _)
    }

    if (optionalElements.nonEmpty)
      mandatoryPattern.flatMap(innerMatch => createPatternMatcher(extractBoundMatchingPairs(innerMatch), true))
    else
      mandatoryPattern
  }

  def createNullValuesForOptionalElements(matchedGraph: Map[String, Any]): Map[String, Null] = {
    (patternGraph.keySet -- matchedGraph.keySet).map(_ -> null).toMap
  }

  private def createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships: Iterable[PatternRelationship], bindings: Map[String, Any]) = {
    val toList = undirectedBoundRelationships.map(patternRel => {
      val rel = bindings(patternRel.key).asInstanceOf[Relationship]
      val x = patternRel.key -> MatchingPair(patternRel, rel)

      // Outputs the first direction of the pattern relationship
      val a1 = patternRel.startNode.key -> MatchingPair(patternRel.startNode, rel.getStartNode)
      val a2 = patternRel.endNode.key -> MatchingPair(patternRel.endNode, rel.getEndNode)

      // Outputs the second direction of the pattern relationship
      val b1 = patternRel.startNode.key -> MatchingPair(patternRel.startNode, rel.getEndNode)
      val b2 = patternRel.endNode.key -> MatchingPair(patternRel.endNode, rel.getStartNode)

      Seq(Map(x, a1, a2), Map(x, b1, b2))
    }).toList
    cartesian(toList).map(_.reduceLeft(_ ++ _))
  }

  private def createPatternMatcher(boundPairs: Map[String, MatchingPair], includeOptionals: Boolean): scala.Traversable[Map[String, Any]] = {

    val patternMatcher = new PatternMatcher(boundPairs, clauses, includeOptionals)

    if (includeOptionals)
      patternMatcher.map(matchedGraph => matchedGraph ++ createNullValuesForOptionalElements(matchedGraph))
    else
      patternMatcher
  }

  // This method takes  a Seq of Seq and produces the cartesian product of all inner Seqs
  // I'm committing this code, but it's all Tobias' doing.
  private def cartesian[T](lst: Seq[Seq[T]]): Seq[Seq[T]] =
    lst.foldRight(List(List[T]()))(// <- the type T needs to be specified here
      (element: Seq[T], result: List[List[T]]) => // types for better readability
        result.flatMap(r => element.map(e => e :: r))
    ).toSeq

  /*
  This method is mutable, but it is only called from the constructor of this class. The created pattern graph
   is immutable and thread safe.
   */
  private def buildPatternGraph(): (Map[String, PatternElement], Set[String]) = {
    val patternNodeMap: scala.collection.mutable.Map[String, PatternNode] = scala.collection.mutable.Map()
    val patternRelMap: scala.collection.mutable.Map[String, PatternRelationship] = scala.collection.mutable.Map()

    boundIdentifiers.identifiers.foreach(_ match {
      case NodeIdentifier(nodeName) => patternNodeMap(nodeName) = new PatternNode(nodeName)
      case _ =>
    })

    patterns.foreach(_ match {
      case RelatedTo(left, right, rel, relType, dir, optional) => {
        val leftNode: PatternNode = patternNodeMap.getOrElseUpdate(left, new PatternNode(left))
        val rightNode: PatternNode = patternNodeMap.getOrElseUpdate(right, new PatternNode(right))

        if (patternRelMap.contains(rel)) {
          throw new SyntaxException("Can't re-use pattern relationship '%s' with different start/end nodes.".format(rel))
        }

        patternRelMap(rel) = leftNode.relateTo(rel, rightNode, relType, dir, optional)
      }
      case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, dir, iterableRel, optional) => {
        val startNode: PatternNode = patternNodeMap.getOrElseUpdate(start, new PatternNode(start))
        val endNode: PatternNode = patternNodeMap.getOrElseUpdate(end, new PatternNode(end))
        patternRelMap(pathName) = startNode.relateViaVariableLengthPathTo(pathName, endNode, minHops, maxHops, relType, dir, optional)
      }
      case _ =>
    })

    val (elementsMap, optionalElements) = validatePattern(patternNodeMap.toMap, patternRelMap.toMap, boundIdentifiers)

    (elementsMap, optionalElements)
  }


  private def extractBoundMatchingPairs(bindings: Map[String, Any]): Map[String, MatchingPair] = {
    bindings.flatMap(kv =>
      if (!classOf[PropertyContainer].isInstance(kv._2))
        Seq()
      else {

        val patternElement = patternGraph.get(kv._1)

        patternElement match {
          case None => Seq()
          case Some(element) => {
            kv._2 match {
              case node: Node => {
                Seq(kv._1 -> MatchingPair(element, node))
              }
              case rel: Relationship => {
                val pr = element.asInstanceOf[PatternRelationship]

                val x = pr.dir match {
                  case Direction.OUTGOING => Some((pr.startNode, pr.endNode))
                  case Direction.INCOMING => Some((pr.endNode, pr.startNode))
                  case Direction.BOTH => None
                }

                //We only want directed bound relationships. Undirected relationship patterns
                //have to be treated a little differently
                x match {
                  case Some((a, b)) => {
                    val t1 = a.key -> MatchingPair(a, rel.getStartNode)
                    val t2 = b.key -> MatchingPair(b, rel.getEndNode)
                    val t3 = pr.key -> MatchingPair(pr, rel)

                    Seq(t1, t2, t3)
                  }
                  case None => Seq()
                }
              }
            }
          }
        }


      })
  }

  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, PatternRelationship],
                              bindings: SymbolTable): (Map[String, PatternElement], Set[String]) = {
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

    def visit(x: PatternElement) {
      if (!visited.contains(x)) {
        visited = visited ++ Seq(x)
        x match {
          case nod: PatternNode => nod.relationships.foreach(visit)
          case rel: PatternRelationship => {
            visit(rel.startNode)
            visit(rel.endNode)
          }
        }
      }
    }

    bindings.identifiers.foreach(id => {
      val el = elementsMap.get(id.name)
      el match {
        case None =>
        case Some(x) => {
          visit(x)
          markMandatoryElements(x)
        }
      }
    })

    val notVisited = elementsMap.values.filterNot(visited contains)

    if (notVisited.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + notVisited.map(_.key).mkString("", ", ", ""))
    }

    (elementsMap, optionalElements.toSet)
  }

}