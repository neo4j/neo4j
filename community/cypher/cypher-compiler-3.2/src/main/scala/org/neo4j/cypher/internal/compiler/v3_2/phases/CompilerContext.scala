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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.CodeGenConfiguration
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{GeneratedQuery, PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContext, CompilationPhaseTracer, InternalNotificationLogger}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition}

case class CompilerContext(exceptionCreator: (String, InputPosition) => CypherException,
                           tracer: CompilationPhaseTracer,
                           notificationLogger: InternalNotificationLogger,
                           planContext: PlanContext,
                           typeConverter: RuntimeTypeConverter,
                           createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                           monitors: Monitors,
                           metrics: Metrics,
                           queryGraphSolver: QueryGraphSolver,
                           config: CypherCompilerConfiguration,
                           updateStrategy: UpdateStrategy,
                           clock: Clock,
                           codeStructure: CodeStructure[GeneratedQuery],
                           codeGenConfiguration: CodeGenConfiguration) extends BaseContext

