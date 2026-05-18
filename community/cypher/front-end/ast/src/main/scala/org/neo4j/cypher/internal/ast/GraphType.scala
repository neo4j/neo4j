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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.AlterCurrentGraphType.Alter
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType.Drop
import org.neo4j.cypher.internal.ast.GraphType.NotDefinedHere
import org.neo4j.cypher.internal.ast.GraphType.ResolutionStatus
import org.neo4j.cypher.internal.ast.GraphType.Resolved
import org.neo4j.cypher.internal.ast.GraphType.Unresolvable
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.ExistenceConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.GraphTypeConstraintBody
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.PropertyTypeConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineConstraintBody
import org.neo4j.cypher.internal.ast.semantics.CypherTypeChecking
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.error
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics._
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StaticElementTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.collection.immutable.ArraySeq
import scala.collection.mutable

case class GraphType(types: Set[GraphTypeEntry], constraints: Set[GraphTypeConstraint])(val position: InputPosition)
    extends ASTNode with SemanticCheckable
    with SemanticAnalysisTooling {

  private val nodesByVar: Map[Variable, NodeType] = types.collect {
    case n @ NodeType(Some(v), _, _, _, _) => (v, n)
  }.toMap

  private val edgesByVar: Map[Variable, EdgeType] = types.collect {
    case e @ EdgeType(_, Some(v), _, _, _, _) => (v, e)
  }.toMap

  private val nodesByLabel: Map[LabelName, NodeType] = types.collect {
    case n @ NodeType(_, l, _, _, _) => (l, n)
  }.toMap

  private val edgesByType: Map[RelTypeName, EdgeType] = types.collect {
    case e @ EdgeType(_, _, rt, _, _, _) => (rt, e)
  }.toMap

  def resolveEndpoint(nodeTypeReference: NodeTypeReference): ResolutionStatus[NodeTypeReference] =
    nodeTypeReference match {
      case NodeTypeReferenceByVariable(variable) =>
        nodesByVar.get(variable).map(nt =>
          Resolved(NodeTypeReferenceByIdentifyingLabel(nt.identifyingLabel, Some(variable))(nodeTypeReference.position))
        ).getOrElse(Unresolvable)
      case ntrl @ NodeTypeReferenceByLabel(label, typeRef) =>
        nodesByLabel.get(label).map(node =>
          Resolved(
            NodeTypeReferenceByIdentifyingLabel(
              node.identifyingLabel,
              typeRef.orElse(node.variable.map(_.copyId))
            )(nodeTypeReference.position)
          )
        ).getOrElse(Resolved(ntrl))
      case e: EmptyNodeTypeReference => Resolved(EmptyNodeTypeReference()(e.position))
      case n @ NodeTypeReferenceByIdentifyingLabel(lab, _) =>
        if (nodesByLabel.contains(lab)) Resolved(n) else NotDefinedHere
    }

  def resolveEndpoint(edgeTypeReference: EdgeTypeReference): ResolutionStatus[EdgeTypeReference] =
    edgeTypeReference match {
      case EdgeTypeReferenceByVariable(variable) =>
        edgesByVar.get(variable).map(et =>
          Resolved(EdgeTypeReferenceByIdentifyingLabel(et.identifyingLabel, Some(variable))(edgeTypeReference.position))
        ).getOrElse(Unresolvable)
      case e @ EdgeTypeReferenceByIdentifyingLabel(rtn, _) =>
        if (edgesByType.contains(rtn)) Resolved(e) else NotDefinedHere
      case e @ EdgeTypeReferenceByLabel(relType, typeRef) =>
        edgesByType.get(relType).map(rel =>
          Resolved(
            EdgeTypeReferenceByIdentifyingLabel(
              rel.identifyingLabel,
              typeRef.orElse(rel.variable)
            )(edgeTypeReference.position)
          )
        ).getOrElse(Resolved(e))
    }

  /* Implied labels should not contain any identifying labels */
  private def checkImpliedLabels(elem: GraphTypeEntry): SemanticCheck = {
    elem match {
      case NodeType(_, _, impliedLabels, _, _) if impliedLabels.nonEmpty =>
        val clashingLabels: Set[LabelName] = impliedLabels.intersect(nodesByLabel.keySet)
        if (clashingLabels.isEmpty) success
        else {
          error(SemanticError.labelIdentifyingAndImplied(
            clashingLabels.map(_.name).toList,
            clashingLabels.head.position
          ))
        }
      case _ => success
    }
  }

  /* Variables and identifiers should be used only once */
  private def checkIdentifiers(
    elem: GraphTypeEntry,
    identifiers: mutable.Set[StaticElementTypeName],
    variables: mutable.Set[Variable]
  ): SemanticCheck = {
    elem match {
      case NodeType(v, l, _, _, _) =>
        (if (v.isDefined && !variables.add(v.get)) {
           error(SemanticError.duplicateTokensInGraphType(v.get.name, v.get.position))
         } else success) chain
          (if (!identifiers.add(l)) error(SemanticError.duplicateTokensInGraphType(l.name, l.position)) else success)
      case EdgeType(_, v, rt, _, _, _) =>
        (if (v.isDefined && !variables.add(v.get))
           error(SemanticError.duplicateTokensInGraphType(v.get.name, v.get.position))
         else success) chain
          (if (!identifiers.add(rt)) error(SemanticError.duplicateTokensInGraphType(rt.name, rt.position))
           else success)
    }
  }

  private def checkElements(): SemanticCheck = {
    val identifiers = mutable.Set[StaticElementTypeName]()
    val variables = mutable.Set[Variable]()
    semanticCheckFold(types) { t => checkIdentifiers(t, identifiers, variables) chain checkImpliedLabels(t) }
  }

  private def checkConstraints(): SemanticCheck = {
    val uniqueConstraintNames = mutable.Set[String]()

    /* Constraint names should be unique */
    def checkForClashingNames(constraint: GraphTypeConstraint): SemanticCheck = {

      def dupConstraintNameError(name: String): SemanticCheck = {
        error(SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(position.offset, position.line, position.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N67)
                .withParam(GqlParams.StringParam.constr, name)
                .atPosition(position.offset, position.line, position.column)
                .build()
            ).build(),
          s"There already exists a constraint called `$name`.",
          position
        ))
      }

      constraint match {
        case c: GraphTypeConstraintDefinition =>
          if (c.name.isEmpty || uniqueConstraintNames.add(c.name.get)) success
          else dupConstraintNameError(c.name.get)
        case GraphTypeConstraintName(name) =>
          if (uniqueConstraintNames.add(name)) success
          else dupConstraintNameError(name)
      }
    }

    /* We should not have independent constraints on define node / edge types which should be dependent */
    def checkForIndependentConstraintsOnDependentLabels(c: GraphTypeConstraint): SemanticCheck = {
      c match {
        case cd @ GraphTypeConstraintDefinition(
            _,
            n @ NodeTypeReferenceByIdentifyingLabel(l, _),
            _: PropertyTypeConstraint,
            _
          ) =>
          SemanticError.independentConstraintOnDependentElement(cd.kernelesqueConstraintDescriptor, l, n.position)
        case cd @ GraphTypeConstraintDefinition(
            _,
            n @ NodeTypeReferenceByIdentifyingLabel(l, _),
            _: ExistenceConstraint,
            _
          ) =>
          SemanticError.independentConstraintOnDependentElement(cd.kernelesqueConstraintDescriptor, l, n.position)
        case cd @ GraphTypeConstraintDefinition(
            _,
            e @ EdgeTypeReferenceByIdentifyingLabel(rt, _),
            _: PropertyTypeConstraint,
            _
          ) =>
          SemanticError.independentConstraintOnDependentElement(cd.kernelesqueConstraintDescriptor, rt, e.position)
        case cd @ GraphTypeConstraintDefinition(
            _,
            e @ EdgeTypeReferenceByIdentifyingLabel(rt, _),
            _: ExistenceConstraint,
            _
          ) =>
          SemanticError.independentConstraintOnDependentElement(cd.kernelesqueConstraintDescriptor, rt, e.position)
        case _ => success
      }
    }

    semanticCheckFold(constraints)(c =>
      checkForClashingNames(c) chain
        checkForIndependentConstraintsOnDependentLabels(c)
    )
  }

  override def semanticCheck: SemanticCheck =
    semanticCheck(types) chain semanticCheck(constraints) chain checkElements() chain checkConstraints()
}

object GraphType {
  sealed trait ResolutionStatus[+T <: GraphTypeElementReference] {}
  case object Unresolvable extends ResolutionStatus[Nothing]
  case object NotDefinedHere extends ResolutionStatus[Nothing]
  case class Resolved[T <: GraphTypeElementReference](elementReference: T) extends ResolutionStatus[T]
}

sealed trait GraphTypeEntry extends ASTNode with SemanticCheckable

object GraphTypeEntry extends SemanticAnalysisTooling {

  /* Check property types are valid, and check for duplicate keys */
  def checkProperties(props: Set[PropertyType]): SemanticCheck = {
    val keys = mutable.Set[PropertyKeyName]()
    semanticCheckFold(props) { prop =>
      CypherTypeChecking.checkPropertyTypeForGraphType(
        prop.propertyType,
        prop.normalizedPropertyType,
        SemanticError.propertyTypeUnsupportedInConstraint("graph type", _, _, _)
      ) chain {
        if (!keys.add(prop.name)) error(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(prop.position.offset, prop.position.line, prop.position.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC1)
                .withParam(GqlParams.StringParam.propKey, prop.name.name)
                .atPosition(prop.position.offset, prop.position.line, prop.position.column)
                .build()
            ).build(),
          s"duplicate property key `${prop.name.name}`",
          prop.position
        )
        else success
      }
    }
  }

  def checkOptionsMaps(constraints: Set[(GraphTypeConstraintBody, Options)]): SemanticCheck =
    semanticCheckFold(constraints) {
      case (_, options) => options.checkOptionsForSchema("")
    }
}

// Node
case class NodeType(
  variable: Option[Variable],
  identifyingLabel: LabelName,
  additionalLabels: Set[LabelName],
  propertyTypes: Set[PropertyType],
  constraints: Set[(GraphTypeConstraintBody, Options)]
)(val position: InputPosition) extends GraphTypeEntry {

  def checkNotEmpty: SemanticCheck = SemanticCheck.fromState { (state: SemanticState) =>
    if (state.graphTypeMode != Drop) {
      if (propertyTypes.isEmpty && additionalLabels.isEmpty)
        error(SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(position.offset, position.line, position.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC2)
                .withParam(GqlParams.StringParam.label, identifyingLabel.name)
                .atPosition(position.offset, position.line, position.column)
                .build()
            ).build(),
          s"node element type `${identifyingLabel.name}` is empty",
          position
        ))
      else success
    } else success
  }

  override def semanticCheck: SemanticCheck =
    GraphTypeEntry.checkProperties(propertyTypes) chain checkNotEmpty chain GraphTypeEntry.checkOptionsMaps(constraints)
}

// Edge
case class EdgeType(
  src: NodeTypeReference,
  variable: Option[Variable],
  identifyingLabel: RelTypeName,
  propertyTypes: Set[PropertyType],
  dest: NodeTypeReference,
  constraints: Set[(GraphTypeConstraintBody, Options)]
)(val position: InputPosition)
    extends GraphTypeEntry {

  def checkNotEmpty: SemanticCheck = SemanticCheck.fromState { (state: SemanticState) =>
    if (state.graphTypeMode != Drop) {
      if (
        propertyTypes.isEmpty && src.isInstanceOf[EmptyNodeTypeReference] && dest.isInstanceOf[EmptyNodeTypeReference]
      ) {
        error(SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(position.offset, position.line, position.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NC3)
                .withParam(GqlParams.StringParam.relType, identifyingLabel.name)
                .atPosition(position.offset, position.line, position.column)
                .build()
            ).build(),
          s"relationship element type `${identifyingLabel.name}` is empty",
          position
        ))
      } else success
    } else success
  }

  override def semanticCheck: SemanticCheck =
    GraphTypeEntry.checkProperties(propertyTypes) chain checkNotEmpty chain GraphTypeEntry.checkOptionsMaps(constraints)
}

// Property
case class PropertyType(
  name: PropertyKeyName,
  propertyType: CypherType,
  constraint: Option[PropertyInlineConstraintBody]
)(
  val position: InputPosition
) extends ASTNode {
  val normalizedPropertyType: CypherType = CypherType.normalizeTypes(propertyType)
}

object PropertyType {
  sealed trait PropertyInlineConstraintBody extends ASTNode

  case class PropertyInlineKeyConstraint()(val position: InputPosition)
      extends PropertyInlineConstraintBody {}

  case class PropertyInlineUniquenessConstraint()(val position: InputPosition)
      extends PropertyInlineConstraintBody {}
}

// Node / Edge references

sealed trait GraphTypeElementReference extends ASTNode

sealed trait NodeTypeReference extends GraphTypeElementReference

case class EmptyNodeTypeReference()(val position: InputPosition) extends NodeTypeReference

case class NodeTypeReferenceByVariable(typeReference: Variable)(val position: InputPosition) extends NodeTypeReference

case class NodeTypeReferenceByLabel(labelName: LabelName, typeReference: Option[Variable] = None)(
  val position: InputPosition
) extends NodeTypeReference

case class NodeTypeReferenceByIdentifyingLabel(labelName: LabelName, typeReference: Option[Variable] = None)(
  val position: InputPosition
) extends NodeTypeReference

sealed trait EdgeTypeReference extends GraphTypeElementReference

case class EdgeTypeReferenceByVariable(typeReference: Variable)(val position: InputPosition) extends EdgeTypeReference

case class EdgeTypeReferenceByLabel(relTypeName: RelTypeName, typeReference: Option[Variable] = None)(
  val position: InputPosition
) extends EdgeTypeReference

case class EdgeTypeReferenceByIdentifyingLabel(relTypeName: RelTypeName, typeReference: Option[Variable] = None)(
  val position: InputPosition
) extends EdgeTypeReference

// Constraints
sealed trait GraphTypeConstraint extends ASTNode with SemanticCheckable {}

case class GraphTypeConstraintName(name: String)(val position: InputPosition)
    extends GraphTypeConstraint {
  override def semanticCheck: SemanticCheck = success
}

case class GraphTypeConstraintDefinition(
  name: Option[String],
  reference: GraphTypeElementReference,
  body: GraphTypeConstraintBody,
  options: Options
)(val position: InputPosition)
    extends GraphTypeConstraint with SemanticAnalysisTooling {

  val key: GraphTypeConstraintKey = GraphTypeConstraintKey(this)

  override def semanticCheck: SemanticCheck = checkValidMode() chain
    body.semanticCheck chain
    checkProperties(body.properties.map(_.propertyKey)) chain
    checkConstraintReference() chain
    options.checkOptionsForSchema("")

  private def checkValidMode(): SemanticCheck = SemanticCheck.fromState { (state: SemanticState) =>
    if (state.graphTypeMode == Drop || state.graphTypeMode == Alter) {
      error(SemanticError(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(position.offset, position.line, position.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NCF)
              .withParam(GqlParams.StringParam.graphTypeOperation, state.graphTypeMode.name())
              .atPosition(position.offset, position.line, position.column)
              .build()
          ).build(),
        s"Constraint definitions are not supported in the ALTER CURRENT GRAPH TYPE ${state.graphTypeMode.name()} operation.",
        position
      ))
    } else success
  }

  private def checkConstraintReference(): SemanticCheck = {

    def checkPropertyVariablesInScope(
      entityType: String,
      scope: Set[Variable],
      props: Seq[Property]
    ): SemanticCheck =
      semanticCheckFold(props) {
        case p @ Property(Variable(name), _) if !scope.exists(_.name == name) =>
          val errorPos = p.map.position
          error(SemanticError(
            GqlHelper.getGql42001_22NC5(name, entityType, errorPos.offset, errorPos.line, errorPos.column),
            s"graph type element referenced by '$name' not found.",
            errorPos
          ))
        case _ => success
      }

    reference match {
      case NodeTypeReferenceByVariable(v) => checkPropertyVariablesInScope("node", Set(v), body.properties)
      case NodeTypeReferenceByLabel(_, v) => checkPropertyVariablesInScope("node", v.toSet, body.properties)
      case NodeTypeReferenceByIdentifyingLabel(_, v) =>
        checkPropertyVariablesInScope("node", v.toSet, body.properties)
      case EdgeTypeReferenceByVariable(v) => checkPropertyVariablesInScope("relationship", Set(v), body.properties)
      case EdgeTypeReferenceByLabel(_, v) => checkPropertyVariablesInScope("relationship", v.toSet, body.properties)
      case EdgeTypeReferenceByIdentifyingLabel(_, v) =>
        checkPropertyVariablesInScope("relationship", v.toSet, body.properties)
      case EmptyNodeTypeReference() => checkPropertyVariablesInScope("node", Set.empty, body.properties)
    }
  }

  // Check for duplicate property keys in constraint
  private def checkProperties(properties: Seq[PropertyKeyName]): SemanticCheck = {
    val props = mutable.Set[PropertyKeyName]()
    semanticCheckFold(properties) { prop =>
      if (props.add(prop)) success
      else error(
        SemanticError(
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
            .atPosition(prop.position.offset, prop.position.line, prop.position.column)
            .withCause(
              ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N75)
                .withParam(GqlParams.StringParam.constrDescrOrName, kernelesqueConstraintDescriptor)
                .withParam(GqlParams.StringParam.token, prop.name)
                .atPosition(prop.position.offset, prop.position.line, prop.position.column)
                .build()
            ).build(),
          s"duplicate property key `${prop.name}`",
          prop.position
        )
      )
    }
  }

  def kernelesqueConstraintDescriptor: String = {
    val namePart = name.map(n => s"name='$n', ").getOrElse("")
    val constraintTypePart = "type='" + (reference match {
      case _: NodeTypeReference => "NODE " + body.typeDescriptor
      case _: EdgeTypeReference => "RELATIONSHIP " + body.typeDescriptor
    }) + "'"
    val typePart = body match {
      case PropertyTypeConstraint(_, propertyType) => s", propertyType=${propertyType.toCypherTypeString}"
      case _                                       => ""
    }

    s"Constraint( $namePart$constraintTypePart, schema=$constraintSchema$typePart )"
  }

  private def constraintSchema: String = {
    val props = body.properties.map(prop => s"${prop.propertyKey.name}").mkString(", ")
    reference match {
      case NodeTypeReferenceByVariable(v)             => s"(${v.name} {$props})"
      case NodeTypeReferenceByLabel(l, _)             => s"(:${l.name} {$props})"
      case NodeTypeReferenceByIdentifyingLabel(l, _)  => s"(:${l.name} {$props})"
      case EdgeTypeReferenceByVariable(v)             => s"()-[${v.name} {$props}]-()"
      case EdgeTypeReferenceByLabel(rt, _)            => s"()-[:${rt.name} {$props}])-()"
      case EdgeTypeReferenceByIdentifyingLabel(rt, _) => s"()-[:${rt.name} {$props}]-()"
      case EmptyNodeTypeReference()                   => ""
    }
  }
}

object GraphTypeConstraint {

  sealed trait GraphTypeConstraintBody extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

    val properties: ArraySeq[Property]

    def typeDescriptor: String

    def withProperties(newProperties: ArraySeq[Property]): GraphTypeConstraintBody
  }

  case class ExistenceConstraint(override val properties: ArraySeq[Property])(val position: InputPosition)
      extends GraphTypeConstraintBody {
    override def semanticCheck: SemanticCheck = success

    override def typeDescriptor: String = "PROPERTY EXISTENCE"

    override def withProperties(newProperties: ArraySeq[Property]): GraphTypeConstraintBody =
      this.copy(properties = newProperties)(position)
  }

  case class PropertyTypeConstraint(override val properties: ArraySeq[Property], propertyType: CypherType)(
    val position: InputPosition
  ) extends GraphTypeConstraintBody {
    val normalizedPropertyType: CypherType = CypherType.normalizeTypes(propertyType)

    override def semanticCheck: SemanticCheck =
      CypherTypeChecking.checkPropertyTypeForConstraint(
        propertyType,
        normalizedPropertyType,
        SemanticError.propertyTypeUnsupportedInConstraint("graph type", _, _, _)
      )

    override def typeDescriptor: String = "PROPERTY TYPE"

    override def withProperties(newProperties: ArraySeq[Property]): GraphTypeConstraintBody =
      this.copy(properties = newProperties)(position)
  }

  case class KeyConstraint(override val properties: ArraySeq[Property])(val position: InputPosition)
      extends GraphTypeConstraintBody {
    override def semanticCheck: SemanticCheck = success

    override def typeDescriptor: String = "KEY"

    override def withProperties(newProperties: ArraySeq[Property]): GraphTypeConstraintBody =
      this.copy(properties = newProperties)(position)
  }

  case class UniquenessConstraint(override val properties: ArraySeq[Property])(val position: InputPosition)
      extends GraphTypeConstraintBody {
    override def semanticCheck: SemanticCheck = success

    override def typeDescriptor: String = "PROPERTY UNIQUENESS"

    override def withProperties(newProperties: ArraySeq[Property]): GraphTypeConstraintBody =
      this.copy(properties = newProperties)(position)
  }
}
