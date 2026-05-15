/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.GraphTypeStringifier
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.collection.immutable.ArraySeq
import scala.math.Ordering.Implicits.seqOrdering

sealed trait GraphType

object GraphType {

  // Implicit orderings

  implicit object GraphTypeEntryOrdering extends Ordering[GraphTypeEntry] {

    override def compare(x: GraphTypeEntry, y: GraphTypeEntry): Int = (x, y) match {
      case (x: NodeElementType, y: NodeElementType) => x.identifyingLabel.name.compareTo(y.identifyingLabel.name)
      case (_: NodeElementType, _)                  => -1
      case (x: RelationshipElementType, y: RelationshipElementType) =>
        x.identifyingLabel.name.compareTo(y.identifyingLabel.name)
      case (_: RelationshipElementType, _: NodeElementType) => 1
    }
  }

  implicit object PropertyTypeOrdering extends Ordering[PropertyType] {
    override def compare(x: PropertyType, y: PropertyType): Int = x.name.name.compareTo(y.name.name)
  }

  implicit object GraphTypeCreateConstraintOrdering extends Ordering[GraphTypeCreateConstraint] {

    private type SortKey = ((Int, String), Seq[String], Int)

    private val sortLabel: PartialFunction[GraphElementTypeReferenceForConstraint, (Int, String)] = {
      case n: NodeElementTypeReferenceByIdentifyingLabel         => (1, n.labelName.name)
      case r: RelationshipElementTypeReferenceByIdentifyingLabel => (1, r.relTypeName.name)
      case n: NodeElementTypeReferenceByLabel                    => (2, n.labelName.name)
      case r: RelationshipElementTypeReferenceByLabel            => (2, r.relTypeName.name)
    }

    private val sortKey: PartialFunction[GraphTypeCreateConstraint, SortKey] = {
      case GraphTypeCreateConstraint(_, reference, props, KeyConstraint, _) =>
        (sortLabel(reference), props.map(_.name), 1)
      case GraphTypeCreateConstraint(_, reference, props, UniquenessConstraint, _) =>
        (sortLabel(reference), props.map(_.name), 2)
      case GraphTypeCreateConstraint(_, reference, props, ExistenceConstraint, _) =>
        (sortLabel(reference), props.map(_.name), 3)
      case GraphTypeCreateConstraint(_, reference, props, _: PropertyTypeConstraint, _) =>
        (sortLabel(reference), props.map(_.name), 4)
    }

    override def compare(x: GraphTypeCreateConstraint, y: GraphTypeCreateConstraint): Int = (x, y) match {
      // Two node type constraints
      case (
          GraphTypeCreateConstraint(_, _: NodeElementTypeReferenceForConstraint, _, _, _),
          GraphTypeCreateConstraint(_, _: NodeElementTypeReferenceForConstraint, _, _, _)
        ) => implicitly[Ordering[SortKey]].compare(sortKey(x), sortKey(y))
      // First is node, second is edge
      case (GraphTypeCreateConstraint(_, _: NodeElementTypeReferenceForConstraint, _, _, _), _) => -1
      // Two edge type constraints
      case (
          GraphTypeCreateConstraint(_, _: RelationshipElementTypeReferenceForConstraint, _, _, _),
          GraphTypeCreateConstraint(_, _: RelationshipElementTypeReferenceForConstraint, _, _, _)
        ) => implicitly[Ordering[SortKey]].compare(sortKey(x), sortKey(y))
      // First is edge, second is node
      case (GraphTypeCreateConstraint(_, _: RelationshipElementTypeReferenceForConstraint, _, _, _), _) => 1
    }
  }

  implicit object GraphTypeDropConstraintOrdering extends Ordering[GraphTypeDropConstraint] {
    override def compare(x: GraphTypeDropConstraint, y: GraphTypeDropConstraint): Int = x.name.compareTo(y.name)
  }

  // Stringifying graph type, putting it here instead of in LogicalPlan2PlanDescription
  // so that it can later be re-used for SHOW CURRENT GRAPH TYPE.
  // It should fulfill the criteria for PrettyString,
  // only reason PrettyString isn't used is that we can't reach that from here.

  private val stringifier: ExpressionStringifier = ExpressionStringifier.pretty(e => e.asCanonicalStringVal)

  /** Returns the string representation of the graph type given by the element types and constraint specifications.
   *
   * @param elementTypes  The node and relationship element types of the graph type
   * @param constraints   The independent/undesignated constraints to be created of the graph type
   * @return              The canonical form of the graph type `{ ... }`
   */
  def graphTypeInfoForShow(
    elementTypes: Set[GraphTypeEntry],
    constraints: Set[GraphTypeCreateConstraint]
  ): String =
    if (elementTypes.isEmpty && constraints.isEmpty) "{}"
    else {
      // Turn into ast graph type representation to reuse it's stringifier
      val nodeVariable = Variable("n")(InputPosition.NONE, isIsolated = false)
      val relVariable = Variable("r")(InputPosition.NONE, isIsolated = false)
      def getConstraintRef(ref: GraphElementTypeReferenceForConstraint): (ast.GraphTypeElementReference, Variable) =
        ref match {
          case ref: NodeElementTypeReferenceByLabel =>
            (ast.NodeTypeReferenceByLabel(ref.labelName, Some(nodeVariable))(InputPosition.NONE), nodeVariable)
          case ref: NodeElementTypeReferenceByIdentifyingLabel =>
            (
              ast.NodeTypeReferenceByIdentifyingLabel(ref.labelName, Some(nodeVariable))(InputPosition.NONE),
              nodeVariable
            )
          case ref: RelationshipElementTypeReferenceByLabel =>
            (ast.EdgeTypeReferenceByLabel(ref.relTypeName, Some(relVariable))(InputPosition.NONE), relVariable)
          case ref: RelationshipElementTypeReferenceByIdentifyingLabel =>
            (
              ast.EdgeTypeReferenceByIdentifyingLabel(ref.relTypeName, Some(relVariable))(InputPosition.NONE),
              relVariable
            )
        }

      val astElemTypes: Set[ast.GraphTypeEntry] = elementTypes.map {
        case net: NodeElementType         => net.toAstObject
        case ret: RelationshipElementType => ret.toAstObject
      }

      val astConstraints: Set[ast.GraphTypeConstraint] = constraints.map(constraint => {
        val (reference, variable) = getConstraintRef(constraint.reference)
        val astProperties = constraint.properties.map(Property(variable, _)(InputPosition.NONE))

        val constraintBody = constraint.constraintType match {
          case ExistenceConstraint =>
            ast.GraphTypeConstraint.ExistenceConstraint(astProperties)(InputPosition.NONE)
          case ptc: PropertyTypeConstraint =>
            ast.GraphTypeConstraint.PropertyTypeConstraint(astProperties, ptc.propertyType)(InputPosition.NONE)
          case KeyConstraint =>
            ast.GraphTypeConstraint.KeyConstraint(astProperties)(InputPosition.NONE)
          case UniquenessConstraint =>
            ast.GraphTypeConstraint.UniquenessConstraint(astProperties)(InputPosition.NONE)
        }

        ast.GraphTypeConstraintDefinition(
          constraint.name,
          reference,
          constraintBody,
          constraint.options
        )(InputPosition.NONE)
      })

      val astGraphType = ast.GraphType(astElemTypes, astConstraints)(InputPosition.NONE)

      GraphTypeStringifier.apply(astGraphType)
    }

  /** Returns the string representation of the graph type given by the element types and constraint specifications.
   *
   * @param elementTypes  The node and relationship element types of the graph type
   * @param constraints   The independent/undesignated constraints to be created of the graph type
   * @return              The canonical form of the graph type `{ ... }`
   *                      (but on a single line and identifiers are only backticked when needed)
   */
  def graphTypeInfoForPlan(
    elementTypes: Set[GraphTypeEntry],
    constraints: Set[GraphTypeCreateConstraint]
  ): String =
    if (elementTypes.isEmpty && constraints.isEmpty) "{}"
    else {
      val elementTypeStrings = getElementTypesStrings(elementTypes)
      val constraintStrings = getConstraintsStrings(constraints)

      s"{ ${(elementTypeStrings ++ constraintStrings).mkString(", ")} }"
    }

  /** Returns the string representation of the graph type given by the element types and constraint names.
   *
   * @param elementTypes  The node and relationship element types of the graph type
   * @param constraints   The independent/undesignated constraints to be dropped by name of the graph type
   * @return              The canonical form of the graph type `{ ... }`
   */
  def graphTypeDropInfo(
    elementTypes: Set[GraphTypeEntry],
    constraints: Set[GraphTypeDropConstraint]
  ): String =
    if (elementTypes.isEmpty && constraints.isEmpty) "{}"
    else {
      val elementTypeStrings = getElementTypesStrings(elementTypes)
      val constraintStrings = constraints.toList.sorted.map(c =>
        // constraint names are parsed as variables, so use that to get proper prettifying in the stringifier
        s"CONSTRAINT ${stringifier(Variable(c.name)(InputPosition.NONE, Variable.isIsolatedDefault))}"
      )

      s"{ ${(elementTypeStrings ++ constraintStrings).mkString(", ")} }"
    }

  // Returns an ordered list of node and relationship element type strings
  private def getElementTypesStrings(elementTypes: Set[GraphTypeEntry]): List[String] = {
    def getProperties(propTypes: Set[PropertyType]) = propTypes.toList.sorted.map(pt =>
      s"${stringifier(pt.name)} :: ${pt.propertyType.normalizedCypherTypeString()}"
    )
    def getEndpointRefNode(node: NodeElementTypeReferenceForRelationshipElementType) = node match {
      case EmptyNodeElementTypeReference                   => "()"
      case ref: NodeElementTypeReferenceByLabel            => s"(:${stringifier(ref.labelName)})"
      case ref: NodeElementTypeReferenceByIdentifyingLabel => s"(:${stringifier(ref.labelName)} =>)"
    }

    elementTypes.toList.sorted.map {
      case net: NodeElementType =>
        val identifyingLabel = stringifier(net.identifyingLabel)
        val impliedLabels = net.additionalLabels.toList.sortBy(_.name).map(stringifier(_))
        val impliedLabelsString = impliedLabels.mkString(":", "&", "")
        val properties = getProperties(net.propertyTypes)
        val propertiesString = properties.mkString("{", ", ", "}")

        if (impliedLabels.isEmpty && properties.isEmpty) {
          // Both empty, only allowed in DROP
          s"(:$identifyingLabel =>)"
        } else if (impliedLabels.isEmpty) {
          // properties only
          s"(:$identifyingLabel => $propertiesString)"
        } else if (properties.isEmpty) {
          // labels only
          s"(:$identifyingLabel => $impliedLabelsString)"
        } else {
          // both labels and properties
          s"(:$identifyingLabel => $impliedLabelsString $propertiesString)"
        }
      case ret: RelationshipElementType =>
        val identifyingLabel = stringifier(ret.identifyingLabel)
        val sourceNode = getEndpointRefNode(ret.sourceNode)
        val targetNode = getEndpointRefNode(ret.targetNode)
        val properties = getProperties(ret.propertyTypes)
        val propertiesString = properties.mkString("{", ", ", "}")

        if (properties.isEmpty) {
          s"$sourceNode-[:$identifyingLabel =>]->$targetNode"
        } else {
          s"$sourceNode-[:$identifyingLabel => $propertiesString]->$targetNode"
        }
    }
  }

  // Returns an ordered list of constraint strings
  def getConstraintsStrings(constraints: Set[GraphTypeCreateConstraint]): List[String] = {
    constraints.toList.sorted.map(c => {
      val name = c.name.map(n =>
        // constraint names are parsed as variables, so use that to get proper prettifying in the stringifier
        s" ${stringifier(Variable(n)(InputPosition.NONE, Variable.isIsolatedDefault))}"
      ).getOrElse("")
      val (elemType, variable) = c.reference match {
        case n: NodeElementTypeReferenceByLabel =>
          (s"(n:${stringifier(n.labelName)})", "n")
        case n: NodeElementTypeReferenceByIdentifyingLabel =>
          (s"(n:${stringifier(n.labelName)} =>)", "n")
        case r: RelationshipElementTypeReferenceByLabel =>
          (s"()-[r:${stringifier(r.relTypeName)}]->()", "r")
        case r: RelationshipElementTypeReferenceByIdentifyingLabel =>
          (s"()-[r:${stringifier(r.relTypeName)} =>]->()", "r")
      }
      val props = c.properties.map(p => s"$variable.${stringifier(p)}")
      val propertiesString = if (props.size == 1) props.head else props.mkString("(", ", ", ")")
      val assertion = c.constraintType.predicate
      val options = c.options match {
        case ast.NoOptions               => ""
        case ast.OptionsParam(parameter) => s" OPTIONS ${stringifier(parameter)}"
        case ast.OptionsMap(innerMap) =>
          val mapString = innerMap.map({
            case (s, e) =>
              // maps in the expression have PropertyKeyName for the keys,
              // so use that to get proper prettifying in the stringifier of the map keys
              s"${stringifier(PropertyKeyName(s)(InputPosition.NONE))}: ${stringifier(e)}"
          }).mkString("{", ", ", "}")
          s" OPTIONS $mapString"
      }

      s"CONSTRAINT$name FOR $elemType REQUIRE $propertiesString $assertion$options"
    })
  }

  def stringifyPropertyName(property: PropertyKeyName): String = stringifier(property)
}

case class GraphTypeForSet(
  elementTypes: Set[GraphTypeEntry],
  constraints: Set[GraphTypeCreateConstraint]
) extends GraphType

case class GraphTypeForAdd(
  elementTypes: Set[GraphTypeEntry],
  constraints: Set[GraphTypeCreateConstraint]
) extends GraphType

case class GraphTypeForAlter(elementTypes: Set[GraphTypeEntry]) extends GraphType

case class GraphTypeForDrop(
  elementTypes: Set[GraphTypeEntry],
  constraints: Set[GraphTypeDropConstraint]
) extends GraphType

// Node and relationship element types

sealed trait GraphTypeEntry

case class NodeElementType(
  identifyingLabel: LabelName,
  additionalLabels: Set[LabelName],
  propertyTypes: Set[PropertyType]
) extends GraphTypeEntry {

  val isJustAnIdentifier: Boolean = additionalLabels.isEmpty && propertyTypes.isEmpty

  def toAstObject: ast.NodeType = {
    ast.NodeType(
      None,
      identifyingLabel,
      additionalLabels,
      propertyTypes.map(pt =>
        ast.PropertyType(pt.name, pt.propertyType, None)(InputPosition.NONE)
      ),
      Set.empty
    )(InputPosition.NONE)
  }
}

case class RelationshipElementType(
  identifyingLabel: RelTypeName,
  sourceNode: NodeElementTypeReferenceForRelationshipElementType,
  targetNode: NodeElementTypeReferenceForRelationshipElementType,
  propertyTypes: Set[PropertyType]
) extends GraphTypeEntry {

  val isJustAnIdentifier: Boolean = sourceNode == EmptyNodeElementTypeReference &&
    targetNode == EmptyNodeElementTypeReference &&
    propertyTypes.isEmpty

  def isEquivalentForDrop(toDrop: RelationshipElementType): Boolean = {

    def compatibleEndNode(
      existing: NodeElementTypeReferenceForRelationshipElementType,
      toDrop: NodeElementTypeReferenceForRelationshipElementType
    ): Boolean = {
      (existing, toDrop) match {
        case (NodeElementTypeReferenceByIdentifyingLabel(l1), NodeElementTypeReferenceByIdentifyingLabel(l2)) =>
          l1 == l2
        case (NodeElementTypeReferenceByLabel(l1), NodeElementTypeReferenceByLabel(l2))            => l1 == l2
        case (NodeElementTypeReferenceByIdentifyingLabel(l1), NodeElementTypeReferenceByLabel(l2)) => l1 == l2
        case (EmptyNodeElementTypeReference, EmptyNodeElementTypeReference)                        => true
        case _                                                                                     => false
      }
    }

    identifyingLabel == toDrop.identifyingLabel &&
    propertyTypes == toDrop.propertyTypes &&
    compatibleEndNode(sourceNode, toDrop.sourceNode) &&
    compatibleEndNode(targetNode, toDrop.targetNode)
  }

  def toAstObject: ast.EdgeType = {
    def getEndNodeRef(endNode: NodeElementTypeReferenceForRelationshipElementType): ast.NodeTypeReference =
      endNode match {
        case EmptyNodeElementTypeReference =>
          ast.EmptyNodeTypeReference()(InputPosition.NONE)
        case ref: NodeElementTypeReferenceByLabel =>
          ast.NodeTypeReferenceByLabel(ref.labelName)(InputPosition.NONE)
        case ref: NodeElementTypeReferenceByIdentifyingLabel =>
          ast.NodeTypeReferenceByIdentifyingLabel(ref.labelName)(InputPosition.NONE)
      }

    ast.EdgeType(
      getEndNodeRef(sourceNode),
      None,
      identifyingLabel,
      propertyTypes.map(pt =>
        ast.PropertyType(pt.name, pt.propertyType, None)(InputPosition.NONE)
      ),
      getEndNodeRef(targetNode),
      Set.empty
    )(InputPosition.NONE)
  }
}

case class PropertyType(name: PropertyKeyName, propertyType: CypherType)

// References

sealed trait NodeElementTypeReferenceForRelationshipElementType
sealed trait GraphElementTypeReferenceForConstraint

sealed trait NodeElementTypeReferenceForConstraint extends GraphElementTypeReferenceForConstraint {
  def labelName: LabelName
}

sealed trait RelationshipElementTypeReferenceForConstraint extends GraphElementTypeReferenceForConstraint {
  def relTypeName: RelTypeName
}

case object EmptyNodeElementTypeReference extends NodeElementTypeReferenceForRelationshipElementType

case class NodeElementTypeReferenceByLabel(labelName: LabelName)
    extends NodeElementTypeReferenceForRelationshipElementType with NodeElementTypeReferenceForConstraint

case class NodeElementTypeReferenceByIdentifyingLabel(labelName: LabelName)
    extends NodeElementTypeReferenceForRelationshipElementType with NodeElementTypeReferenceForConstraint

case class RelationshipElementTypeReferenceByLabel(relTypeName: RelTypeName)
    extends RelationshipElementTypeReferenceForConstraint

case class RelationshipElementTypeReferenceByIdentifyingLabel(relTypeName: RelTypeName)
    extends RelationshipElementTypeReferenceForConstraint

// Graph type constraints

case class GraphTypeCreateConstraint(
  name: Option[String],
  reference: GraphElementTypeReferenceForConstraint,
  properties: ArraySeq[PropertyKeyName],
  constraintType: GraphTypeConstraintType,
  options: ast.Options
) {

  def isEquivalent(that: GraphTypeCreateConstraint): Boolean =
    this.reference == that.reference && this.properties.sameElements(
      that.properties
    ) && this.constraintType == that.constraintType

  def kernelesqueConstraintDescriptor: String = {
    val propertiesString = properties.map(_.name).mkString(", ")
    val cypherType = constraintType match {
      case PropertyTypeConstraint(ct) => s", propertyType=${ct.normalizedCypherTypeString()}"
      case _                          => ""
    }
    val (cst, schema) = reference match {
      case nr: NodeElementTypeReferenceForConstraint =>
        ("NODE " + constraintType.description, s"(:${nr.labelName.name} {$propertiesString})")
      case rr: RelationshipElementTypeReferenceForConstraint =>
        ("RELATIONSHIP " + constraintType.description, s"()-[:${rr.relTypeName.name} {$propertiesString}]-()")
    }
    s"Constraint( type='$cst', schema=$schema$cypherType )"
  }
}

sealed trait GraphTypeConstraintType {
  def description: String
  def predicate: String
}

case object ExistenceConstraint extends GraphTypeConstraintType {
  override val description: String = "PROPERTY EXISTENCE"
  override val predicate: String = "IS NOT NULL"
}

case class PropertyTypeConstraint(propertyType: CypherType) extends GraphTypeConstraintType {
  override val description: String = "PROPERTY TYPE"
  override val predicate: String = s"IS :: ${propertyType.description}"
}

case object KeyConstraint extends GraphTypeConstraintType {
  override val description: String = "KEY"
  override val predicate: String = "IS KEY"
}

case object UniquenessConstraint extends GraphTypeConstraintType {
  override val description: String = "PROPERTY UNIQUENESS"
  override val predicate: String = "IS UNIQUE"
}

case class GraphTypeDropConstraint(name: String)
