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

sealed trait ShowFunctionType {
  val prettyPrint: String
  val description: String
}

case object AllFunctions extends ShowFunctionType {
  override val prettyPrint: String = "ALL"
  override val description: String = "allFunctions"
}

case object BuiltInFunctions extends ShowFunctionType {
  override val prettyPrint: String = "BUILT IN"
  override val description: String = "builtInFunctions"
}

case object UserDefinedFunctions extends ShowFunctionType {
  override val prettyPrint: String = "USER DEFINED"
  override val description: String = "userDefinedFunctions"
}
