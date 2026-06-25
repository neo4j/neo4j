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
package org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType.AlterOperation
import org.neo4j.cypher.internal.ast.EdgeType
import org.neo4j.cypher.internal.ast.EdgeTypeReference
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByVariable
import org.neo4j.cypher.internal.ast.GraphType
import org.neo4j.cypher.internal.ast.GraphType.NotDefinedHere
import org.neo4j.cypher.internal.ast.GraphType.Resolved
import org.neo4j.cypher.internal.ast.GraphType.Unresolvable
import org.neo4j.cypher.internal.ast.GraphTypeConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.ExistenceConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.GraphTypeConstraintBody
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.PropertyTypeConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraintDefinition
import org.neo4j.cypher.internal.ast.GraphTypeConstraintKey
import org.neo4j.cypher.internal.ast.GraphTypeConstraintName
import org.neo4j.cypher.internal.ast.GraphTypeElementReference
import org.neo4j.cypher.internal.ast.GraphTypeEntry
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NodeType
import org.neo4j.cypher.internal.ast.NodeTypeReference
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByVariable
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.PropertyType
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineConstraintBody
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StaticElementTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.conditions.NoInlineConstraints
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.gqlstatus.GqlHelper

import scala.collection.immutable.ArraySeq
import scala.collection.mutable

/**
 * Rewrite graph type to its canonical form by:
 *  * Replacing references with identifying references
 *  * Convert all inline constraints to top level constraint
 */
case class RewriteGraphTypeReferences(cypherExceptionFactory: CypherExceptionFactory) {

  private val instance: Rewriter = topDown(Rewriter.lift {
    case acgt @ AlterCurrentGraphType(gt @ GraphType(entries, constraints), operation, _) =>
      acgt.copy(graphType =
        rewriteInlineConstraints(gt.copy(
          types = rewriteEntries(operation, gt, entries),
          constraints = rewriteConstraints(operation, gt, constraints)
        )(gt.position))
      )(acgt.position)
  })

  private def rewriteEntries(
    operation: AlterOperation,
    graphType: GraphType,
    entries: Set[GraphTypeEntry]
  ): Set[GraphTypeEntry] = {
    val strict = operation == AlterCurrentGraphType.Set
    entries.map {
      case e: EdgeType => e.copy(
          src = resolveNodeTypeReference(strict, graphType, e.src),
          dest = resolveNodeTypeReference(strict, graphType, e.dest)
        )(e.position)
      case n: NodeType => n
    }
  }

  /**
   * Canonicalise top level constraints
   * @return
   */
  private def rewriteConstraints(
    operation: AlterOperation,
    graphType: GraphType,
    constraints: Set[GraphTypeConstraint]
  ): Set[GraphTypeConstraint] = {
    val strict = operation == AlterCurrentGraphType.Set
    constraints.map {
      case gtc @ GraphTypeConstraintDefinition(_, n: NodeTypeReference, _, _) =>
        gtc.copy(reference = resolveNodeTypeReference(strict, graphType, n))(gtc.position)
      case gtc @ GraphTypeConstraintDefinition(_, e: EdgeTypeReference, _, _) =>
        gtc.copy(reference = resolveEdgeTypeReference(strict, graphType, e))(gtc.position)
      case constraintName: GraphTypeConstraintName => constraintName
    }
  }

  /**
   * Lookup a node type reference in the graph type
   */
  private def resolveNodeTypeReference(
    strict: Boolean,
    graphType: GraphType,
    nodeTypeReference: NodeTypeReference
  ): NodeTypeReference = {
    graphType.resolveEndpoint(nodeTypeReference) match {
      case Unresolvable => throw cypherExceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC5(
            referenceDescriptor(nodeTypeReference),
            "node",
            nodeTypeReference.position.offset,
            nodeTypeReference.position.line,
            nodeTypeReference.position.column
          ),
          s"graph type element referenced by '${referenceDescriptor(nodeTypeReference)}' not found",
          nodeTypeReference.position
        )
      case NotDefinedHere if strict =>
        throw cypherExceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC5(
            referenceDescriptor(nodeTypeReference),
            "node",
            nodeTypeReference.position.offset,
            nodeTypeReference.position.line,
            nodeTypeReference.position.column
          ),
          s"graph type element referenced by '${referenceDescriptor(nodeTypeReference)}' not found",
          nodeTypeReference.position
        )
      case NotDefinedHere => nodeTypeReference
      case Resolved(ntr)  => ntr
    }
  }

  /**
   * Lookup an edge type reference in the graph type
   */
  private def resolveEdgeTypeReference(
    strict: Boolean,
    graphType: GraphType,
    edgeTypeReference: EdgeTypeReference
  ): EdgeTypeReference = {
    graphType.resolveEndpoint(edgeTypeReference) match {
      case Unresolvable => throw cypherExceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC5(
            referenceDescriptor(edgeTypeReference),
            "relationship",
            edgeTypeReference.position.offset,
            edgeTypeReference.position.line,
            edgeTypeReference.position.column
          ),
          s"graph type element referenced by '${referenceDescriptor(edgeTypeReference)}' not found",
          edgeTypeReference.position
        )
      case NotDefinedHere if strict =>
        throw cypherExceptionFactory.syntaxException(
          GqlHelper.getGql42001_22NC5(
            referenceDescriptor(edgeTypeReference),
            "relationship",
            edgeTypeReference.position.offset,
            edgeTypeReference.position.line,
            edgeTypeReference.position.column
          ),
          s"graph type element referenced by '${referenceDescriptor(edgeTypeReference)}' not found",
          edgeTypeReference.position
        )
      case NotDefinedHere => edgeTypeReference
      case Resolved(etr)  => etr
    }
  }

  private val referenceDescriptor: PartialFunction[GraphTypeElementReference, String] = {
    case NodeTypeReferenceByVariable(v)              => v.name
    case NodeTypeReferenceByIdentifyingLabel(lab, _) => s"(:`${lab.name}` =>)"
    case EdgeTypeReferenceByVariable(v)              => v.name
    case EdgeTypeReferenceByIdentifyingLabel(lab, _) => s"()-[:`${lab.name}` =>]->()"
  }

  /**
   * Convert all the inline constraints on the graph type to top level constraints.
   */
  private def rewriteInlineConstraints(graphType: GraphType): GraphType = {

    def extractElementConstraints[E <: StaticElementTypeName : ReferenceBuilder](
      propertyTypes: Set[PropertyType],
      v: Option[Variable],
      name: E,
      elementTypeConstraints: Set[(GraphTypeConstraintBody, Options)]
    ): (Set[PropertyType], List[GraphTypeConstraint]) = {
      val (pts, consts) = propertyTypes.toList.map(extractPropertyTypeInlineConstraint).unzip
      val newConsts: List[GraphTypeConstraint] = consts.flatten.map { case (propertyKeyName, inlineBody) =>
        inlineConstraintToConstraint(v, name, propertyKeyName, inlineBody)
      } ++ elementLevelConstraintToConstraint(v, name, elementTypeConstraints)
      (pts.toSet, newConsts)
    }

    val (newEntries, constraints): (Set[GraphTypeEntry], Set[List[GraphTypeConstraint]]) = graphType.types.map {
      case nt @ NodeType(variable, name, _, propertyTypes, nodeTypeConstraints) =>
        val (pts, newConsts) = extractElementConstraints(propertyTypes, variable, name, nodeTypeConstraints)
        (nt.copy(propertyTypes = pts, constraints = Set.empty)(nt.position), newConsts)
      case et @ EdgeType(_, variable, name, propertyTypes, _, edgeTypeConstraints) =>
        val (pts, newConsts) = extractElementConstraints(propertyTypes, variable, name, edgeTypeConstraints)
        (et.copy(propertyTypes = pts, constraints = Set.empty)(et.position), newConsts)
    }.unzip

    // Check for duplicate constraints after rewriting, because there are multiple ways to express the same constraint
    // in a graph type.
    val newConstraints: mutable.Map[GraphTypeConstraintKey, GraphTypeConstraintDefinition] = mutable.Map()
    val (oldConstraints, oldConstraintNames) =
      graphType.constraints.toList.partition(_.isInstanceOf[GraphTypeConstraintDefinition])
    (oldConstraints ++ constraints.toList.flatten).foreach {
      case c: GraphTypeConstraintDefinition =>
        val existingConstraint = newConstraints.get(c.key)
        if (existingConstraint.isDefined) {
          throwExistingConstraintException(c, existingConstraint.get)
        } else {
          newConstraints.put(c.key, c)
        }
      case _ => ()
    }

    graphType.copy(types = newEntries, newConstraints.values.toSet ++ oldConstraintNames)(graphType.position)
  }

  /**
   *  Take constraint bodies attached to a graph type element and convert it into a top level constraint
   *
   * @param elementVariable the element type alias of the element
   * @param name label / relationship type
   * @param bodies the constraint bodies to convert
   */
  private def elementLevelConstraintToConstraint[E <: StaticElementTypeName](
    elementVariable: Option[Variable],
    name: E,
    bodies: Set[(GraphTypeConstraintBody, Options)]
  )(implicit refBuilder: ReferenceBuilder[E]): Set[GraphTypeConstraint] = {
    bodies.map {
      case (body, options) =>
        GraphTypeConstraintDefinition(
          None,
          refBuilder(name, elementVariable),
          body,
          options
        )(body.position)
    }
  }

  /**
   * Split a property type into it's property and it's constraint
   */
  private def extractPropertyTypeInlineConstraint(propertyType: PropertyType)
    : (PropertyType, Option[(PropertyKeyName, PropertyInlineConstraintBody)]) = propertyType match {
    case pt @ PropertyType(_, _, Some(const)) => (pt.copy(constraint = None)(pt.position), Some((pt.name, const)))
    case _                                    => (propertyType, None)
  }

  private trait ReferenceBuilder[T <: StaticElementTypeName] {
    def apply(e: T, v: Option[Variable]): GraphTypeElementReference
    def variableNameGenerator(): Variable
  }

  implicit private val n: ReferenceBuilder[LabelName] = new ReferenceBuilder[LabelName] {
    override def apply(n: LabelName, v: Option[Variable]): GraphTypeElementReference =
      NodeTypeReferenceByIdentifyingLabel(n, v.map(_.copyId))(n.position)
    override def variableNameGenerator(): Variable = Variable("n")(InputPosition.NONE, Variable.isIsolatedDefault)
  }

  implicit private val e: ReferenceBuilder[RelTypeName] = new ReferenceBuilder[RelTypeName] {
    override def apply(e: RelTypeName, v: Option[Variable]): GraphTypeElementReference =
      EdgeTypeReferenceByIdentifyingLabel(e, v.map(_.copyId))(e.position)
    override def variableNameGenerator(): Variable = Variable("r")(InputPosition.NONE, Variable.isIsolatedDefault)
  }

  /**
    * Convert a constraint attached to a property to be a top level constraint
    */
  private def inlineConstraintToConstraint[E <: StaticElementTypeName](
    variable: Option[Variable],
    name: E,
    propertyKeyName: PropertyKeyName,
    body: PropertyInlineConstraintBody
  )(implicit refBuilder: ReferenceBuilder[E]): GraphTypeConstraint = {
    val v = variable.map(_.copyId).getOrElse(refBuilder.variableNameGenerator())
    body match {
      case c: PropertyInlineKeyConstraint => GraphTypeConstraintDefinition(
          None,
          refBuilder(name, Some(v)),
          KeyConstraint(ArraySeq(Property(v, propertyKeyName)(propertyKeyName.position)))(c.position),
          NoOptions
        )(body.position)
      case c: PropertyInlineUniquenessConstraint => GraphTypeConstraintDefinition(
          None,
          refBuilder(name, Some(v)),
          UniquenessConstraint(ArraySeq(Property(v, propertyKeyName)(propertyKeyName.position)))(c.position),
          NoOptions
        )(body.position)
    }
  }

  private def throwExistingConstraintException(
    newConstraint: GraphTypeConstraintDefinition,
    existingConstraint: GraphTypeConstraintDefinition
  ): Unit = {

    def areEquivalent(
      newConstraint: GraphTypeConstraintDefinition,
      existingConstraint: GraphTypeConstraintDefinition
    ): Boolean = {
      (newConstraint.body, existingConstraint.body) match {
        case (_: KeyConstraint, _: KeyConstraint)               => true
        case (_: UniquenessConstraint, _: UniquenessConstraint) => true
        case (PropertyTypeConstraint(_, cypherType1), PropertyTypeConstraint(_, cypherType2))
          if cypherType1 == cypherType2 => true
        case (_: ExistenceConstraint, _: ExistenceConstraint) => true
        case _                                                => false
      }
    }

    if (areEquivalent(newConstraint, existingConstraint)) {
      throw throw cypherExceptionFactory.syntaxException(
        GqlHelper.getGql42001_22N65(
          newConstraint.kernelesqueConstraintDescriptor,
          newConstraint.position.offset,
          newConstraint.position.line,
          newConstraint.position.column
        ),
        s"An equivalent constraint already exists, '${newConstraint.kernelesqueConstraintDescriptor}'",
        newConstraint.position
      )
    } else {
      throw throw cypherExceptionFactory.syntaxException(
        GqlHelper.getGql42001_22N66(
          newConstraint.kernelesqueConstraintDescriptor,
          newConstraint.position.offset,
          newConstraint.position.line,
          newConstraint.position.column
        ),
        s"Conflicting constraint already exists: '${newConstraint.kernelesqueConstraintDescriptor}'",
        newConstraint.position
      )
    }
  }
}

object RewriteGraphTypeReferences extends Step with DefaultPostCondition with PreparatoryRewritingRewriterFactory {

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    versionOpt: Option[CypherVersion]
  ): Rewriter =
    RewriteGraphTypeReferences(cypherExceptionFactory: CypherExceptionFactory).instance

  /**
   * @return the conditions that need to be met before this step can be allowed to run.
   */
  override def preConditions: Set[Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(NoInlineConstraints)

  /**
   * @return the conditions that this step invalidates as a side effect of its work.
   */
  override def invalidatedConditions: Set[Condition] = Set.empty

}
