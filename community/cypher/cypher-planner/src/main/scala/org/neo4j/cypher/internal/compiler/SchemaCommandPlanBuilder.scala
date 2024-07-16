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
package org.neo4j.cypher.internal.compiler

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyTypeConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipKeyConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyTypeConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateVectorNodeIndex
import org.neo4j.cypher.internal.ast.CreateVectorRelationshipIndex
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeKey
import org.neo4j.cypher.internal.logical.plans.NodePropertyExistence
import org.neo4j.cypher.internal.logical.plans.NodePropertyType
import org.neo4j.cypher.internal.logical.plans.NodeUniqueness
import org.neo4j.cypher.internal.logical.plans.RelationshipKey
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyExistence
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyType
import org.neo4j.cypher.internal.logical.plans.RelationshipUniqueness
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.graphdb.schema.IndexType

/**
 * This planner takes on queries that requires no planning such as schema commands
 */
case object SchemaCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()

    def handleIfExistsDo(
      entityName: ElementTypeName,
      props: List[Property],
      indexType: IndexType,
      name: Option[Either[String, Parameter]],
      ifExistsDo: IfExistsDo,
      options: Options
    ): (List[PropertyKeyName], Option[DoNothingIfExistsForIndex]) = {
      val propKeys = props.map(_.propertyKey)
      val source = ifExistsDo match {
        case IfExistsDoNothing => Some(plans.DoNothingIfExistsForIndex(entityName, propKeys, indexType, name, options))
        case _                 => None
      }
      (propKeys, source)
    }

    def createRangeIndex(
      entityName: ElementTypeName,
      props: List[Property],
      name: Option[Either[String, Parameter]],
      ifExistsDo: IfExistsDo,
      options: Options
    ): Option[LogicalPlan] = {
      val (propKeys, source) = handleIfExistsDo(entityName, props, IndexType.RANGE, name, ifExistsDo, options)
      Some(plans.CreateIndex(source, IndexType.RANGE, entityName, propKeys, name, options))
    }

    def createFulltextIndex(
      entityNames: Either[List[LabelName], List[RelTypeName]],
      props: List[Property],
      name: Option[Either[String, Parameter]],
      ifExistsDo: IfExistsDo,
      options: Options
    ): Option[LogicalPlan] = {
      val propKeys = props.map(_.propertyKey)
      val source = ifExistsDo match {
        case IfExistsDoNothing => Some(plans.DoNothingIfExistsForFulltextIndex(entityNames, propKeys, name, options))
        case _                 => None
      }
      Some(plans.CreateFulltextIndex(source, entityNames, propKeys, name, options))
    }

    def createTextIndex(
      entityName: ElementTypeName,
      props: List[Property],
      name: Option[Either[String, Parameter]],
      ifExistsDo: IfExistsDo,
      options: Options
    ): Option[LogicalPlan] = {
      val (propKeys, source) = handleIfExistsDo(entityName, props, IndexType.TEXT, name, ifExistsDo, options)
      Some(plans.CreateIndex(source, IndexType.TEXT, entityName, propKeys, name, options))
    }

    def createPointIndex(
      entityName: ElementTypeName,
      props: List[Property],
      name: Option[Either[String, Parameter]],
      ifExistsDo: IfExistsDo,
      options: Options
    ): Option[LogicalPlan] = {
      val (propKeys, source) = handleIfExistsDo(entityName, props, IndexType.POINT, name, ifExistsDo, options)
      Some(plans.CreateIndex(source, IndexType.POINT, entityName, propKeys, name, options))
    }

    def createVectorIndex(
      entityName: ElementTypeName,
      props: List[Property],
      name: Option[Either[String, Parameter]],
      ifExistsDo: IfExistsDo,
      options: Options
    ): Option[LogicalPlan] = {
      val (propKeys, source) = handleIfExistsDo(entityName, props, IndexType.VECTOR, name, ifExistsDo, options)
      Some(plans.CreateIndex(source, IndexType.VECTOR, entityName, propKeys, name, options))
    }

    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS NODE KEY [OPTIONS {...}]
      case CreateNodeKeyConstraint(_, label, props, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              label,
              props,
              plans.NodeKey,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, NodeKey, label, props, name, options))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1,rel.prop2) IS RELATIONSHIP KEY [OPTIONS {...}]
      case CreateRelationshipKeyConstraint(_, relType, props, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              relType,
              props,
              plans.RelationshipKey,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, RelationshipKey, relType, props, name, options))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS UNIQUE [OPTIONS {...}]
      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS UNIQUE [OPTIONS {...}]
      case CreateNodePropertyUniquenessConstraint(_, label, props, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              label,
              props,
              plans.NodeUniqueness,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, NodeUniqueness, label, props, name, options))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE rel.prop IS UNIQUE [OPTIONS {...}]
      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1,rel.prop2) IS UNIQUE [OPTIONS {...}]
      case CreateRelationshipPropertyUniquenessConstraint(_, relType, props, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              relType,
              props,
              plans.RelationshipUniqueness,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, RelationshipUniqueness, relType, props, name, options))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS NOT NULL
      case CreateNodePropertyExistenceConstraint(_, label, prop, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              label,
              Seq(prop),
              plans.NodePropertyExistence,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, NodePropertyExistence, label, Seq(prop), name, options))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              relType,
              Seq(prop),
              plans.RelationshipPropertyExistence,
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(source, RelationshipPropertyExistence, relType, Seq(prop), name, options))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS :: ...
      case c @ CreateNodePropertyTypeConstraint(_, label, prop, _, name, ifExistsDo, options, _) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              label,
              Seq(prop),
              plans.NodePropertyType(c.normalizedPropertyType),
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(
          source,
          NodePropertyType(c.normalizedPropertyType),
          label,
          Seq(prop),
          name,
          options
        ))

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS :: ...
      case c @ CreateRelationshipPropertyTypeConstraint(
          _,
          relType,
          prop,
          _,
          name,
          ifExistsDo,
          options,
          _
        ) =>
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForConstraint(
              relType,
              Seq(prop),
              plans.RelationshipPropertyType(c.normalizedPropertyType),
              name,
              options
            ))
          case _ => None
        }
        Some(plans.CreateConstraint(
          source,
          RelationshipPropertyType(c.normalizedPropertyType),
          relType,
          Seq(prop),
          name,
          options
        ))

      // DROP CONSTRAINT name [IF EXISTS]
      case DropConstraintOnName(name, ifExists, _) =>
        Some(plans.DropConstraintOnName(name, ifExists))

      // CREATE [RANGE] INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
      case CreateRangeNodeIndex(_, label, props, name, ifExistsDo, options, _, _) =>
        createRangeIndex(label, props, name, ifExistsDo, options)

      // CREATE [RANGE] INDEX [name] [IF NOT EXISTS] FOR ()-[r:RELATIONSHIP_TYPE]->() ON (r.prop) [OPTIONS {...}]
      case CreateRangeRelationshipIndex(_, relType, props, name, ifExistsDo, options, _, _) =>
        createRangeIndex(relType, props, name, ifExistsDo, options)

      // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR (n) ON EACH labels(n)
      // CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR ()-[r]-() ON [EACH] type(r)
      case CreateLookupIndex(_, isNodeIndex, _, name, ifExistsDo, options, _) =>
        val entityType = if (isNodeIndex) EntityType.NODE else EntityType.RELATIONSHIP
        val source = ifExistsDo match {
          case IfExistsDoNothing => Some(plans.DoNothingIfExistsForLookupIndex(entityType, name, options))
          case _                 => None
        }
        Some(plans.CreateLookupIndex(source, entityType, name, options))

      // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR (n[:LABEL[|...]]) ON EACH (n.prop[, ...]) [OPTIONS {...}]
      case CreateFulltextNodeIndex(_, labels, props, name, ifExistsDo, options, _) =>
        createFulltextIndex(Left(labels), props, name, ifExistsDo, options)

      // CREATE FULLTEXT INDEX [name] [IF NOT EXISTS] FOR ()-[r[:RELATIONSHIP_TYPE[|...]]]->() ON EACH (r.prop[, ...]) [OPTIONS {...}]
      case CreateFulltextRelationshipIndex(_, relTypes, props, name, ifExistsDo, options, _) =>
        createFulltextIndex(Right(relTypes), props, name, ifExistsDo, options)

      // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
      case CreateTextNodeIndex(_, label, props, name, ifExistsDo, options, _) =>
        createTextIndex(label, props, name, ifExistsDo, options)

      // CREATE TEXT INDEX [name] [IF NOT EXISTS] FOR ()-[r:RELATIONSHIP_TYPE]->() ON (r.prop) [OPTIONS {...}]
      case CreateTextRelationshipIndex(_, relType, props, name, ifExistsDo, options, _) =>
        createTextIndex(relType, props, name, ifExistsDo, options)

      // CREATE POINT INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) [OPTIONS {...}]
      case CreatePointNodeIndex(_, label, props, name, ifExistsDo, options, _) =>
        createPointIndex(label, props, name, ifExistsDo, options)

      // CREATE POINT INDEX [name] [IF NOT EXISTS] FOR ()-[r:RELATIONSHIP_TYPE]->() ON (r.prop) [OPTIONS {...}]
      case CreatePointRelationshipIndex(_, relType, props, name, ifExistsDo, options, _) =>
        createPointIndex(relType, props, name, ifExistsDo, options)

      // CREATE VECTOR INDEX [name] [IF NOT EXISTS] FOR (n:LABEL) ON (n.prop) OPTIONS {...}
      case CreateVectorNodeIndex(_, label, props, name, ifExistsDo, options, _) =>
        createVectorIndex(label, props, name, ifExistsDo, options)

      // CREATE VECTOR INDEX [name] [IF NOT EXISTS] FOR ()-[r:RELATIONSHIP_TYPE]->() ON (r.prop) OPTIONS {...}
      case CreateVectorRelationshipIndex(_, relType, props, name, ifExistsDo, options, _) =>
        createVectorIndex(relType, props, name, ifExistsDo, options)

      // DROP INDEX name [IF EXISTS]
      case DropIndexOnName(name, ifExists, _) =>
        Some(plans.DropIndexOnName(name, ifExists))

      case _ => None
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = AdministrationPlannerName)
    else planState
  }
}
