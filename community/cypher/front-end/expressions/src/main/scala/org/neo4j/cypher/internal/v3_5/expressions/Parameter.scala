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
package org.neo4j.cypher.internal.v3_5.expressions

import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.symbols._

sealed trait Param {

  self: Expression =>

  def parameterName: String

  override def asCanonicalStringVal: String
}

case class Parameter(name: String,
                     parameterType: CypherType)(val position: InputPosition)
  extends Expression with Param {

  override def asCanonicalStringVal: String = "$" + name

  override def parameterName: String = name
}

case class ParameterWithOldSyntax(name: String,
                                  parameterType: CypherType)(val position: InputPosition)
  extends Expression with Param {

  override def asCanonicalStringVal: String = "$" + name

  override def parameterName: String = name
}
