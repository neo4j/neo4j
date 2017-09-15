/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticDirection
import org.scalacheck.Gen._
import org.scalacheck.{Gen, Shrink}
import org.scalatest.prop.PropertyChecks

trait PatternGen extends  PropertyChecks {
  protected def minPatternLength = 2
  protected def maxPatternLength = 8
  protected def numberOfTestRuns = 100
  protected def maxDiscardedInputs = 500
  protected def maxSize = 10

  override implicit val generatorDrivenConfig = PropertyCheckConfig(
    minSuccessful = numberOfTestRuns, maxDiscarded = maxDiscardedInputs, maxSize = maxSize
  )

  val nameSeq = new AtomicInteger

  implicit val shrinkPattern: Shrink[List[Element]] = Shrink { elements =>
    // use the default utility for shrinking of lists
    Shrink.shrinkContainer[List, Element].shrink(elements).map { shrink =>
      // find standalone node
      val standaloneNode = shrink.collectFirst {
        case nr: Node => nr
      }

      if (standaloneNode.isDefined) {
        // either move it to the end of pattern
        shrink.filter(_ != standaloneNode.get) :+ standaloneNode.get
      } else  if (shrink.nonEmpty) {
        // or strip trailing relationship
        shrink.dropRight(1) :+ shrink.last.asInstanceOf[NodeWithRelationship].node
      } else {
        shrink
      }
    }
  }

  def numberOfNamedNodes(elements: List[Element]): Int = elements.count {
    // either pattern with named node
    case NodeWithRelationship(NamedNode(_), _) => true
    case NodeWithRelationship(NamedLabeledNode(_, _), _) => true
    // or single named node
    case nn: NamedNode => true
    case nln: NamedLabeledNode => true
    case _ => false
  }

  def findFirstNodeName(elements: List[Element]): Option[String] = elements.collectFirst {
    // either name from node that participates in pattern
    case NodeWithRelationship(NamedNode(name), _) => name
    case NodeWithRelationship(NamedLabeledNode(name, _), _) => name
    // or name from single node
    case NamedNode(name) => name
    case NamedLabeledNode(name, _) => name
  }

  def findAllNodeNames(elements: List[Element]): Seq[String] = {

    def findName(element: Element): Option[String] = element match {
      // either name from node that participates in pattern
      case NodeWithRelationship(node, _) => findName(node)
      // or name from single node
      case NamedNode(name) => Some(name)
      case NamedLabeledNode(name, _) => Some(name)
      case NamedLabeledNodeWithProperties(name, _, _) => Some(name)
      case _ => None
    }

    elements.flatMap(findName)
  }

  def findAllRelationshipNames(elements: List[Element]): Seq[String] = {

    def findName(element: Element): Option[String] = element match {
      case NodeWithRelationship(_, rel) => findName(rel)
      case NamedRelationship(name, _) => Some(name)
      case NamedRelationshipWithLength(name, _, _) => Some(name)
      case NamedTypedRelationship(name, _, _) => Some(name)
      case NamedTypedWithPropertiesRelationship(name, _, _, _) => Some(name)
      case _ => None
    }

    elements.flatMap(findName)
  }

  sealed trait Element {
    val string: String
  }

  sealed trait Node extends Element

  sealed trait Relationship extends Element {
    def direction: SemanticDirection
    def withDirection(direction: SemanticDirection): Relationship
  }

  case class NodeWithRelationship(node: Node, rel: Relationship) extends Element {
    val string = node.string + rel.string
  }

  case class EmptyNode() extends Node {
    val string = "()"
  }

  case class NamedNode(name: String) extends Node {
    val string = s"($name)"
  }

  case class LabeledNode(label: String) extends Node {
    val string = s"(:$label)"
  }

  case class LabeledNodeWithProperties(label: Seq[String], property: Seq[String]) extends Node {
    val string = s"(${label.map(":" + _).mkString("")} {${property.mkString(",")}})"
  }

  case class NamedLabeledNode(name: String, label: String) extends Node {
    val string = s"($name:$label)"
  }

  case class NamedLabeledNodeWithProperties(name: String, label: Seq[String], property: Seq[String]) extends Node {
    val string = s"($name ${label.map(":" + _).mkString("")} {${property.mkString(",")}})"
  }

  case class EmptyRelationship(direction: SemanticDirection) extends Relationship {
    val string = formatRelationship("", direction)

    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class EmptyRelationshipWithLength(length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$length]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class NamedRelationship(name: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class NamedRelationshipWithLength(name: String, length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name $length]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class TypedRelationship(relType: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[:$relType]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class TypedWithPropertiesRelationship(relType: String, direction: SemanticDirection, properties: Seq[String]) extends Relationship {
    val string = formatRelationship(s"[:$relType {${properties.mkString(",")}}]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class TypedRelationshipWithLength(relType: String, length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[:$relType $length]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class NamedTypedRelationship(name: String, relType: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name:$relType]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class NamedTypedWithPropertiesRelationship(name: String, relType: String, direction: SemanticDirection, properties: Seq[String]) extends Relationship {
    val string = formatRelationship(s"[$name :$relType {${properties.mkString(",")}}]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  case class NamedTypedRelationshipWithLength(name: String, relType: String, length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name:$relType $length]", direction)
    override def withDirection(direction: SemanticDirection) = copy(direction = direction)
  }

  def formatRelationship(definition: String, direction: SemanticDirection) = direction match {
    case BOTH => s"-$definition-"
    case INCOMING => s"<-$definition-"
    case OUTGOING => s"-$definition->"
  }

  def patterns = Gen.choose(minPatternLength, maxPatternLength).flatMap(patternGen)

  def patternGen(maxLength: Int): Gen[List[Element]] =
    if (maxLength == 0)
      nodeGen.map(f => List(f))
    else
      for {
        node <- nodeGen
        rel <- relGen
        other <- patternGen(maxLength - 1)
      } yield NodeWithRelationship(node, rel) :: other

  def relGen: Gen[Relationship]

  def emptyRelGen = relDirection.map(EmptyRelationship)

  def emptyRelWithLengthGen =
    for {
      direction <- relDirection
      length <- relLength
    } yield EmptyRelationshipWithLength(length, direction)

  def namedRelGen =
    for {
      name <- relName
      direction <- relDirection
    } yield NamedRelationship(name, direction)

  def namedRelWithLengthGen =
    for {
      name <- relName
      length <- relLength
      direction <- relDirection
    } yield NamedRelationshipWithLength(name, length, direction)

  def typedRelGen =
    for {
      relType <- relTypeName
      direction <- relDirection
    } yield TypedRelationship(relType, direction)

  def typedWithPropertiesRelGen =
    for {
      relType <- relTypeName
      direction <- relDirection
      properties <- Gen.nonEmptyListOf(property)
    } yield TypedWithPropertiesRelationship(relType, direction, properties)

  def typedRelWithLengthGen =
    for {
      relType <- relTypeName
      length <- relLength
      direction <- relDirection
    } yield TypedRelationshipWithLength(relType, length, direction)

  def namedTypedRelGen =
    for {
      name <- relName
      relType <- relTypeName
      direction <- relDirection
    } yield NamedTypedRelationship(name, relType, direction)

  def namedTypedWithPropertiesRelGen =
    for {
      name <- relName
      relType <- relTypeName
      direction <- relDirection
      properties <- Gen.nonEmptyListOf(property)
    } yield NamedTypedWithPropertiesRelationship(name, relType, direction, properties)

  def namedTypedRelWithLengthGen =
    for {
      name <- relName
      relType <- relTypeName
      length <- relLength
      direction <- relDirection
    } yield NamedTypedRelationshipWithLength(name, relType, length, direction)


  def nodeGen: Gen[Node]

  def emptyNodeGen = Gen.const(EmptyNode())

  def namedNodeGen = nodeName.map(NamedNode)

  def labeledNodeGen = labelName.map(LabeledNode)

  def labeledWithPropertiesNodeGen =
    for {
      label <- Gen.nonEmptyListOf(labelName)
      prop <- Gen.nonEmptyListOf(property)
    } yield LabeledNodeWithProperties(label, prop)

  def namedLabeledWithPropertiesNodeGen =
    for {
      name <- nodeName
      label <- Gen.nonEmptyListOf(labelName)
      prop <- Gen.nonEmptyListOf(property)
    } yield NamedLabeledNodeWithProperties(name, label, prop)

  def namedLabeledNodeGen =
    for {
      name <- nodeName
      label <- labelName
    } yield NamedLabeledNode(name, label)

  def nodeName = alphaLowerChar.map(c => s"n${nameSeq.getAndIncrement()}")

  def labelName = alphaUpperChar.map(c => s"L${nameSeq.getAndIncrement()}")

  def property = alphaUpperChar.map(c => s"prop${nameSeq.getAndIncrement()}:${nameSeq.getAndIncrement()}")

  def relName = alphaLowerChar.map(c => s"r${nameSeq.getAndIncrement()}")

  def relTypeName = alphaUpperChar.map(c => s"T${nameSeq.getAndIncrement()}")

  def relLength = Gen.oneOf(relLengthWithoutBounds, relLengthWithLowerBound, relLengthWithUpperBound,
    relLengthWithLowerAndUpperBound)

  def relLengthWithoutBounds = Gen.const("*")

  def relLengthWithLowerBound = relLengthBound.map(b => s"*$b")

  def relLengthWithUpperBound = relLengthBound.map(b => s"*..$b")

  def relLengthWithLowerAndUpperBound =
    for {
      lower <- relLengthBound
      upper <- relLengthBound
    } yield s"*$lower..$upper"

  def relLengthBound = Gen.choose(1, 10)

  def relDirection = Gen.oneOf(BOTH, INCOMING, OUTGOING)
}
