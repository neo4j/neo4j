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
package org.neo4j.cypher.internal.util.helpers

import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

import scala.util.matching.Regex

object NameDeduplicator {

  object NamedVariable {

    def unapply(variableName: String): Option[String] =
      variableName match {
        case DEDUP_PATTERN(variableName, _)                                       => Some(variableName)
        case variableName if AnonymousVariableNameGenerator.isNamed(variableName) => Some(variableName)
        case _                                                                    => None
      }
  }

  private def nameGeneratorRegex(generatorName: String): Regex =
    s""" {2}($generatorName)(-?\\d+)""".r

  val UNNAMED_PATTERN: Regex = {
    nameGeneratorRegex(AnonymousVariableNameGenerator.generatorName)
  }

  private val UNNAMED_PARAMS_PATTERN = """ {2}(AUTOINT|AUTODOUBLE|AUTOSTRING|AUTOLIST)(\d+)""".r
  val DEDUP_PATTERN: Regex = """ {2}((?:(?! {2}).)+?)@(\d+)""".r

  private def transformGeneratedNamesRewriter(transformation: String => String): Rewriter = topDown(Rewriter.lift {
    case s: String => transformation(s)
  })

  private val deduplicateVariableNames: String => String =
    fixedPoint { DEDUP_PATTERN.replaceAllIn(_, "$1") }

  /**
   * Removes planner-generated uniquely identifying elements from Strings.
   *
   * E.g. the String "  var@23" becomes "var".
   */
  def removeGeneratedNamesAndParams(s: String): String = {
    val paramNamed = UNNAMED_PARAMS_PATTERN.replaceAllIn(s, m => s"${(m group 1).toLowerCase()}_${m group 2}")
    val named = UNNAMED_PATTERN.replaceAllIn(paramNamed, m => s"anon_${m group 2}")

    deduplicateVariableNames(named)
  }

  /**
   * Replaces planner-generated uniquely identifying variable names with empty string.
   *
   * E.g. the String "  UNNAMED23" becomes "".
   */
  def eraseGeneratedNames(s: String): String = UNNAMED_PATTERN.replaceAllIn(s, "")

  /**
   * Removes planner-generated uniquely identifying elements from any Strings found while traversing the tree of the given argument.
   */
  def removeGeneratedNamesAndParamsOnTree[M <: AnyRef](a: M): M = {
    transformGeneratedNamesRewriter(removeGeneratedNamesAndParams).apply(a).asInstanceOf[M]
  }

  def eraseGeneratedNamesOnTree[M <: AnyRef](a: M): M = {
    transformGeneratedNamesRewriter(eraseGeneratedNames).apply(a).asInstanceOf[M]
  }
}
