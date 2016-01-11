/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutablePlanBuilder, ExecutionPlan, PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{FieldSignature, PlanContext, ProcedureName, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{CompilationPhaseTracer, PreparedQuery}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{CallProcedure, CreateIndex, CreateNodePropertyExistenceConstraint, CreateRelationshipPropertyExistenceConstraint, CreateUniquePropertyConstraint, DropIndex, DropNodePropertyExistenceConstraint, DropRelationshipPropertyExistenceConstraint, DropUniquePropertyConstraint, Expression, LabelName, ProcName, PropertyKeyName, RelTypeName}
import org.neo4j.cypher.internal.frontend.v3_0.{CypherTypeException, InvalidArgumentException, SemanticTable}
import org.neo4j.graphdb.QueryExecutionType.QueryType

/**
  * This planner takes on queries that requires no planning such as procedures and schema commands
  *
  * @param delegate The plan builder to delegate to
  */
case class DelegatingProcedureExecutablePlanBuilder(delegate: ExecutablePlanBuilder) extends ExecutablePlanBuilder {

  override def producePlan(inputQuery: PreparedQuery, planContext: PlanContext, tracer: CompilationPhaseTracer,

                           createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference): ExecutionPlan = {

    inputQuery.statement match {

      // CALL foo.bar.baz("arg1", 2)
      case CallProcedure(namespace, ProcName(name), args) =>
        val signature = planContext.procedureSignature(ProcedureName(namespace, name))
        if (args.nonEmpty && args.size != signature.inputSignature.size)
          throw new InvalidArgumentException(
            s"""Procedure ${signature.name.name} takes ${signature.inputSignature.size}
                  |arguments but ${args.size} was provided.""".stripMargin)
        args.zip(signature.inputSignature).foreach {
          case (arg, field) => typeCheck(inputQuery.semanticTable)(arg, field)
        }

        CallProcedureExecutionPlan(signature, args)

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      case CreateUniquePropertyConstraint(node, label, prop) =>
        PureSideEffectExecutionPlan("CreateUniqueConstraint", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.createUniqueConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      case DropUniquePropertyConstraint(_, label, prop) =>
        PureSideEffectExecutionPlan("DropUniqueConstraint", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.dropUniqueConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case CreateNodePropertyExistenceConstraint(_, label, prop) =>
        PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.createNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case DropNodePropertyExistenceConstraint(_, label, prop) =>
        PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        PureSideEffectExecutionPlan("CreateRelationshipPropertyExistenceConstraint", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        })

      // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case DropRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        PureSideEffectExecutionPlan("DropRelationshipPropertyExistenceConstraint", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        })

      case CreateIndex(label, prop) =>
        PureSideEffectExecutionPlan("CreateIndex", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.addIndexRule _).tupled(labelProp(ctx)(label, prop))
        })

      case DropIndex(label, prop) =>
        PureSideEffectExecutionPlan("DropIndex", QueryType.SCHEMA_WRITE, (ctx) => {
          (ctx.dropIndexRule _).tupled(labelProp(ctx)(label, prop))
        })


      case _ => delegate.producePlan(inputQuery, planContext, tracer, createFingerprintReference)
    }
  }

  private def labelProp(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typeProp(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typeCheck(semanticTable: SemanticTable)(exp: Expression, field: FieldSignature) = {
    if (!(semanticTable.types(exp).actual containsAny field.typ.covariant))
      throw new CypherTypeException(
        s"""${field.name} expects ${field.typ}, but got ${semanticTable.types(exp).actual.mkString(",", "or")}""")
  }
}

