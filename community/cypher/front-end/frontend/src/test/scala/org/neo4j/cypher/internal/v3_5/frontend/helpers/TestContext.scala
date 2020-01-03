/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.frontend.helpers

import org.neo4j.cypher.internal.v3_5.frontend.phases.{BaseContext, CompilationPhaseTracer, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.v3_5.util.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.v3_5.util.{CypherException, InputPosition}
import org.scalatest.mock.MockitoSugar.mock

//noinspection TypeAnnotation
case class TestContext(override val notificationLogger: InternalNotificationLogger = mock[InternalNotificationLogger]) extends BaseContext {
  override def tracer = CompilationPhaseTracer.NO_TRACING

  override object exceptionCreator extends ((String, InputPosition) => CypherException) {
    override def apply(msg: String, pos: InputPosition): CypherException = throw new CypherException() {
      override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
        mapper.internalException(msg, this)
    }
  }

  override def monitors = mock[Monitors]

  override def errorHandler = _ => ()
}
