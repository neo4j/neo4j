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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan.procs

import org.neo4j.cypher.internal.compiler.v3_0.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{ExecutablePlanBuilder, ExecutionPlan, PlanFingerprint, PlanFingerprintReference, SCHEMA_WRITE}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{FieldSignature, PlanContext, ProcedureSignature, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_0.{CompilationPhaseTracer, PreparedQuerySemantics, SyntaxExceptionCreator}
import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.symbols.TypeSpec

/**
  * This planner takes on queries that requires no planning such as procedures and schema commands
  *
  * @param delegate The plan builder to delegate to
  */
case class DelegatingProcedureExecutablePlanBuilder(delegate: ExecutablePlanBuilder, publicTypeConverter: Any => Any) extends ExecutablePlanBuilder {

  override def producePlan(inputQuery: PreparedQuerySemantics, planContext: PlanContext, tracer: CompilationPhaseTracer,
                           createFingerprintReference: (Option[PlanFingerprint]) => PlanFingerprintReference): ExecutionPlan = {

    inputQuery.statement match {

      // Global call: CALL foo.bar.baz("arg1", 2)
      case Query(None, SingleQuery(Seq(resolved@ResolvedCall(signature, args, _, _, _)))) =>
        val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
        val mkException = new SyntaxExceptionCreator(inputQuery.queryText, inputQuery.offset)
        errors.foreach { error => throw mkException(error.msg, error.position) }

        ProcedureCallExecutionPlan(signature, args, resolved.callResultTypes, resolved.callResultIndices, publicTypeConverter)

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      case CreateUniquePropertyConstraint(node, label, prop) =>
        PureSideEffectExecutionPlan("CreateUniqueConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.createUniqueConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE
      case DropUniquePropertyConstraint(_, label, prop) =>
        PureSideEffectExecutionPlan("DropUniqueConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.dropUniqueConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // CREATE CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case CreateNodePropertyExistenceConstraint(_, label, prop) =>
        PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.createNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // DROP CONSTRAINT ON (node:Label) ASSERT node.prop EXISTS
      case DropNodePropertyExistenceConstraint(_, label, prop) =>
        PureSideEffectExecutionPlan("CreateNodePropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.dropNodePropertyExistenceConstraint _).tupled(labelProp(ctx)(label, prop.propertyKey))
        })

      // CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case CreateRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        PureSideEffectExecutionPlan("CreateRelationshipPropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.createRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        })

      // DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop EXISTS
      case DropRelationshipPropertyExistenceConstraint(_, relType, prop) =>
        PureSideEffectExecutionPlan("DropRelationshipPropertyExistenceConstraint", SCHEMA_WRITE, (ctx) => {
          (ctx.dropRelationshipPropertyExistenceConstraint _).tupled(typeProp(ctx)(relType, prop.propertyKey))
        })

      // CREATE INDEX ON :LABEL(prop)
      case CreateIndex(label, prop) =>
        PureSideEffectExecutionPlan("CreateIndex", SCHEMA_WRITE, (ctx) => {
          (ctx.addIndexRule _).tupled(labelProp(ctx)(label, prop))
        })

      // DROP INDEX ON :LABEL(prop)
      case DropIndex(label, prop) =>
        PureSideEffectExecutionPlan("DropIndex", SCHEMA_WRITE, (ctx) => {
          (ctx.dropIndexRule _).tupled(labelProp(ctx)(label, prop))
        })

      case _ => delegate.producePlan(inputQuery, planContext, tracer, createFingerprintReference)
    }
  }

  private def labelProp(ctx: QueryContext)(label: LabelName, prop: PropertyKeyName) =
    (ctx.getOrCreateLabelId(label.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typeProp(ctx: QueryContext)(relType: RelTypeName, prop: PropertyKeyName) =
    (ctx.getOrCreateRelTypeId(relType.name), ctx.getOrCreatePropertyKeyId(prop.name))

  private def typeCheck(semanticTable: SemanticTable)(exp: Expression, field: FieldSignature, proc: ProcedureSignature) = {
    val actual = semanticTable.types(exp).actual
    val expected = field.typ
    val intersected = actual intersectOrCoerce expected.covariant
    if (intersected == TypeSpec.none)
      throw new CypherTypeException(
        s"""Parameter `${field.name}` for procedure `${proc.name}`
            |expects value of type $expected but got value of type ${actual.toShortString}.
            |
        |Usage: CALL ${proc.name}(${proc.inputSignature.map(s => s"<${s.name}>").mkString(", ")})
            |${proc.inputSignature.map(s => s"    ${s.name} (type ${s.typ})").mkString("Parameters:" + System.lineSeparator(), System.lineSeparator(),"")}
        """.stripMargin)
  }
}




