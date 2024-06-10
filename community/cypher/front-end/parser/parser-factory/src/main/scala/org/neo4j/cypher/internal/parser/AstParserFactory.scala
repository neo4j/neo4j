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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.parser.ast.AstParser
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.parser.v6.ast.factory.Cypher6AstParser
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InternalNotificationLogger

trait AstParserFactory {

  def apply(
    query: String,
    cypherExceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger]
  ): AstParser
}

object AstParserFactory {

  def apply(version: CypherVersion): AstParserFactory = version match {
    case CypherVersion.Cypher5 => Cypher5AstParserFactory
    case CypherVersion.Cypher6 => Cypher6AstParserFactory
  }
}

object Cypher5AstParserFactory extends AstParserFactory {

  override def apply(
    query: String,
    cypherExceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger]
  ): AstParser = new Cypher5AstParser(query, cypherExceptionFactory, notificationLogger)
}

object Cypher6AstParserFactory extends AstParserFactory {

  override def apply(
    query: String,
    cypherExceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger]
  ): AstParser = new Cypher6AstParser(query, cypherExceptionFactory, notificationLogger)
}
