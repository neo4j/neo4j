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
package org.neo4j.cypher.internal.rewriting.rewriters.astRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.GQLAliasFunctionNameRewritten
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites GQL Alias functions to their Cypher equivalent
 * e.g
 * char_length(STRING) :: INTEGER -> character_length(STRING) :: INTEGER
 * upper(STRING) :: STRING -> toUpper(STRING) :: STRING
 * lower(STRING) :: STRING -> toLower(STRING) :: STRING
 */
case object GQLAliasFunctionNameRewriter extends StepSequencer.Step with PreparatoryRewritingRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(!FunctionInvocationsResolved)

  override def postConditions: Set[Condition] = Set(GQLAliasFunctionNameRewritten)

  override def invalidatedConditions: Set[Condition] = Set.empty

  private val GQLFunctionAliases: Map[String, String] = Map(
    "char_length" -> "character_length",
    "upper" -> "toUpper",
    "lower" -> "toLower",
    "ceiling" -> "ceil",
    "local_time" -> "localtime",
    "local_datetime" -> "localdatetime",
    "zoned_time" -> "time",
    "zoned_datetime" -> "datetime",
    "path_length" -> "length",
    "ln" -> "log",
    "collect_list" -> "collect",
    "percentile_disc" -> "percentileDisc",
    "percentile_cont" -> "percentileCont",
    "stdev_pop" -> "stDevP",
    "stdev_samp" -> "stDev",
    "duration_between" -> "duration.between"
  )

  private def functionNameForTarget(
    name: String,
    position: InputPosition
  ): FunctionName = {
    val parts = name.split('.')
    if (parts.length == 1) {
      FunctionName(name)(position)
    } else {
      val namespace = Namespace(parts.dropRight(1).toList)(position)
      FunctionName(namespace, parts.last)(position)
    }
  }

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case f @ FunctionInvocation(FunctionName(namespace, name), _, _, _, _, _, _)
      if namespace.parts.isEmpty && GQLFunctionAliases.exists(gqlAlias => name.equalsIgnoreCase(gqlAlias._1)) =>
      val targetName = GQLFunctionAliases(name.toLowerCase)
      f.copy(functionName = functionNameForTarget(targetName, f.position))(f.position)
  })

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    versionOpt: Option[CypherVersion]
  ): Rewriter = instance
}
