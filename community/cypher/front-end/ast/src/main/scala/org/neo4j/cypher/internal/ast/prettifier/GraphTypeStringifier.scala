/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.EdgeType
import org.neo4j.cypher.internal.ast.EdgeTypeReference
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByLabel
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByVariable
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.GraphType
import org.neo4j.cypher.internal.ast.GraphTypeConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.ExistenceConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.GraphTypeConstraintBody
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.PropertyTypeConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraintDefinition
import org.neo4j.cypher.internal.ast.GraphTypeConstraintName
import org.neo4j.cypher.internal.ast.GraphTypeElementReference
import org.neo4j.cypher.internal.ast.GraphTypeEntry
import org.neo4j.cypher.internal.ast.NodeType
import org.neo4j.cypher.internal.ast.NodeTypeReference
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByLabel
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByVariable
import org.neo4j.cypher.internal.ast.PropertyType
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.BASE_INDENT
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.NL
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable

import scala.math.Ordering.Implicits.seqOrdering

object GraphTypeStringifier {

  private val es: ExpressionStringifier = ExpressionStringifier(alwaysBacktick = true, alwaysParens = true)

  implicit object GraphTypeEntryOrdering extends Ordering[GraphTypeEntry] {

    override def compare(x: GraphTypeEntry, y: GraphTypeEntry): Int = (x, y) match {
      case (x: NodeType, y: NodeType) => x.identifyingLabel.name.compareTo(y.identifyingLabel.name)
      case (_: NodeType, _)           => -1
      case (x: EdgeType, y: EdgeType) => x.identifyingLabel.name.compareTo(y.identifyingLabel.name)
      case (_: EdgeType, _: NodeType) => 1
    }
  }

  // for ordering graph type constraints
  implicit object GraphTypeConstraintOrdering extends Ordering[GraphTypeConstraint] {

    type SortKey = ((Int, String), Seq[String], Int)

    val sortLabel: PartialFunction[GraphTypeElementReference, (Int, String)] = {
      case NodeTypeReferenceByIdentifyingLabel(l, _) => (1, l.name)
      case EdgeTypeReferenceByIdentifyingLabel(l, _) => (1, l.name)
      case NodeTypeReferenceByVariable(v)            => (2, v.name) // Non-canonical, but identifying, so use 2
      case EdgeTypeReferenceByVariable(v)            => (2, v.name) // Non-canonical, but identifying, so use 2
      case NodeTypeReferenceByLabel(l, _)            => (3, l.name)
      case EdgeTypeReferenceByLabel(l, _)            => (3, l.name)
    }

    val sortKey: PartialFunction[GraphTypeConstraint, SortKey] = {
      case GraphTypeConstraintDefinition(_, reference, KeyConstraint(props), _) =>
        (sortLabel(reference), props.map(_.propertyKey.name), 1)
      case GraphTypeConstraintDefinition(_, reference, UniquenessConstraint(props), _) =>
        (sortLabel(reference), props.map(_.propertyKey.name), 2)
      case GraphTypeConstraintDefinition(_, reference, ExistenceConstraint(props), _) =>
        (sortLabel(reference), props.map(_.propertyKey.name), 3)
      case GraphTypeConstraintDefinition(_, reference, PropertyTypeConstraint(props, _), _) =>
        (sortLabel(reference), props.map(_.propertyKey.name), 4)
    }

    override def compare(x: GraphTypeConstraint, y: GraphTypeConstraint): Int = (x, y) match {
      // Two node type constraints
      case (
          GraphTypeConstraintDefinition(_, _: NodeTypeReference, _, _),
          GraphTypeConstraintDefinition(_, _: NodeTypeReference, _, _)
        ) => implicitly[Ordering[SortKey]].compare(sortKey(x), sortKey(y))
      // First is node, second is edge
      case (GraphTypeConstraintDefinition(_, _: NodeTypeReference, _, _), _) => -1
      // Two edge type constraints
      case (
          GraphTypeConstraintDefinition(_, _: EdgeTypeReference, _, _),
          GraphTypeConstraintDefinition(_, _: EdgeTypeReference, _, _)
        ) => implicitly[Ordering[SortKey]].compare(sortKey(x), sortKey(y))
      // First is edge, second is node
      case (GraphTypeConstraintDefinition(_, _: EdgeTypeReference, _, _), _) => 1
      case (GraphTypeConstraintName(n1), GraphTypeConstraintName(n2))        => n1 compareTo n2
      case (GraphTypeConstraintName(_), _) => 1 // Names go at the end - is this correct
    }
  }

  // For ordering in-lined constraints
  implicit object GraphTypeConstraintBodyOrdering extends Ordering[GraphTypeConstraintBody] {

    val sortKey: PartialFunction[GraphTypeConstraintBody, (Seq[String], Int)] = {
      case KeyConstraint(props)             => (props.map(_.propertyKey.name), 1)
      case UniquenessConstraint(props)      => (props.map(_.propertyKey.name), 2)
      case ExistenceConstraint(props)       => (props.map(_.propertyKey.name), 3)
      case PropertyTypeConstraint(props, _) => (props.map(_.propertyKey.name), 4)
    }

    override def compare(x: GraphTypeConstraintBody, y: GraphTypeConstraintBody): Int =
      implicitly[Ordering[(Seq[String], Int)]].compare(sortKey(x), sortKey(y))
  }

  implicit object PropertyTypeOrdering extends Ordering[PropertyType] {
    override def compare(x: PropertyType, y: PropertyType): Int = x.name.name.compareTo(y.name.name)
  }

  def apply(graphType: GraphType): String = {
    val graphTypeEntries = graphType.types.toList.sorted.map(entry => stringifyEntry(entry))
    val graphTypeConstraints =
      graphType.constraints.toList.sorted.map(entry => stringifyGraphTypeConstraint(entry))
    s"""{
       |$BASE_INDENT${(graphTypeEntries ++ graphTypeConstraints).mkString(s",$NL$BASE_INDENT")}
       |}""".stripMargin
  }

  private def stringifyEntry(
    graphTypeEntry: GraphTypeEntry
  ): String = graphTypeEntry match {
    case nodeType: NodeType => stringifyNodeType(nodeType)
    case edgeType: EdgeType => stringifyEdgeType(edgeType)
  }

  def stringifyNodeType(nodeType: NodeType): String = {
    val propTypes = nodeType.propertyTypes.toList.sorted.map(apply) match {
      case props if props.isEmpty => ""
      case props                  => props.mkString(" {", ", ", "}")
    }
    val secondaryLabels = nodeType.additionalLabels.toList.sortBy(_.name).map(es.apply) match {
      case labels if labels.isEmpty => ""
      case labels                   => labels.mkString(" :", "&", "")
    }

    val constraints = nodeType.constraints.toList.sortBy(_._1).map { case (const, options) =>
      stringifyGraphTypeConstraintBody(const) + Prettifier.stringifyOptions(options)(es)
    }
    val constraintsStr = if (constraints.isEmpty) "" else constraints.mkString(" ", " ", "")

    s"(${variable(nodeType.variable)}:${es.apply(nodeType.identifyingLabel)} =>$secondaryLabels$propTypes)$constraintsStr"
  }

  def stringifyEdgeType(
    edgeType: EdgeType
  ): String = {
    val propTypes = edgeType.propertyTypes.toList.sorted.map(apply) match {
      case props if props.isEmpty => ""
      case props                  => props.mkString(" {", ", ", "}")
    }

    val constraints = edgeType.constraints.toList.sortBy(_._1).map { case (const, options) =>
      stringifyGraphTypeConstraintBody(const) + Prettifier.stringifyOptions(options)(es)
    }
    val constraintsStr = if (constraints.isEmpty) "" else constraints.mkString(" ", " ", "")

    s"${stringifyElementReference(edgeType.src)}-[${variable(edgeType.variable)}:${es.apply(
        edgeType.identifyingLabel
      )} =>$propTypes]->${stringifyElementReference(edgeType.dest)}$constraintsStr"
  }

  private def stringifyGraphTypeConstraint(constraint: GraphTypeConstraint) =
    constraint match {
      case c: GraphTypeConstraintDefinition => stringifyGraphTypeConstraintDefinition(c)
      case GraphTypeConstraintName(name)    => s"CONSTRAINT ${es.backtick(name)}"
    }

  private def stringifyGraphTypeConstraintDefinition(
    constraint: GraphTypeConstraintDefinition
  ): String = {

    val constraintSpecificSuffix = stringifyGraphTypeConstraintBody(constraint.body)

    val constraintName = constraint.name.map(n => s"${es.backtick(n)} ").getOrElse("")
    val options = Prettifier.stringifyOptions(constraint.options)(es)
    s"CONSTRAINT ${constraintName}FOR ${stringifyElementReference(constraint.reference)} $constraintSpecificSuffix$options"
  }

  private def stringifyGraphTypeConstraintBody(
    constaintBody: GraphTypeConstraint.GraphTypeConstraintBody
  ): String = {

    def props(props: Seq[Property]): String =
      props.map(prop => s"${es.apply(prop.map)}.${es.apply(prop.propertyKey)}").mkString(", ")

    constaintBody match {
      case e: PropertyTypeConstraint =>
        s"REQUIRE (${props(e.properties)}) IS :: ${e.normalizedPropertyType.description}"
      case e: KeyConstraint => s"REQUIRE (${props(e.properties)}) IS KEY"
      case e: UniquenessConstraint =>
        s"REQUIRE (${props(e.properties)}) IS UNIQUE"
      case e: ExistenceConstraint => s"REQUIRE (${props(e.properties)}) IS NOT NULL"
    }
  }

  private def apply(propertyType: PropertyType): String = {
    val constraint = propertyType.constraint match {
      case None                                       => ""
      case Some(PropertyInlineUniquenessConstraint()) => " IS UNIQUE"
      case Some(PropertyInlineKeyConstraint())        => " IS KEY"
    }
    s"${es.apply(propertyType.name)} :: ${propertyType.normalizedPropertyType.description}$constraint"
  }

  private def stringifyElementReference(
    edgeEndpointLabel: GraphTypeElementReference
  ): String = {

    edgeEndpointLabel match {
      case _: EmptyNodeTypeReference                          => "()"
      case NodeTypeReferenceByIdentifyingLabel(name, None)    => s"(:${es.apply(name)} =>)"
      case NodeTypeReferenceByIdentifyingLabel(name, Some(v)) => s"(${es.apply(v)}:${es.apply(name)} =>)"
      case NodeTypeReferenceByLabel(name, None)               => s"(:${es.apply(name)})"
      case NodeTypeReferenceByLabel(name, Some(v))            => s"(${es.apply(v)}:${es.apply(name)})"
      case NodeTypeReferenceByVariable(name)                  => s"(${es.apply(name)})"
      case EdgeTypeReferenceByIdentifyingLabel(name, None)    => s"()-[:${es.apply(name)} =>]->()"
      case EdgeTypeReferenceByIdentifyingLabel(name, Some(v)) => s"()-[${es.apply(v)}:${es.apply(name)} =>]->()"
      case EdgeTypeReferenceByLabel(name, None)               => s"()-[:${es.apply(name)}]->()"
      case EdgeTypeReferenceByLabel(name, Some(v))            => s"()-[${es.apply(v)}:${es.apply(name)}]->()"
      case EdgeTypeReferenceByVariable(v)                     => s"()-[${es.apply(v)}]->()"
    }
  }

  private def variable(variable: Option[Variable]): String = {
    variable match {
      case Some(v) => es.apply(v)
      case None    => ""
    }
  }

}
