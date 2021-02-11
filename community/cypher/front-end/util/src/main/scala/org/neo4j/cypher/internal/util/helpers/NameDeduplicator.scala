/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.util.helpers

import org.neo4j.cypher.internal.util.AllNameGenerators.generators
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

import scala.util.matching.Regex

object NameDeduplicator {
  val UNNAMED_PATTERN: Regex = {
    val gens = generators.map(_.generatorName).mkString("|")
    s""" {2}($gens)(\\d+)(?:\\(.*?\\))?""".r
  }

  private val UNNAMED_PARAMS_PATTERN = """ {2}(AUTOINT|AUTODOUBLE|AUTOSTRING|AUTOLIST)(\d+)(?:\(.*?\))?""".r
  private val DEDUP_PATTERN = """ {2}([^\s]+)@\d+(?:\(.*?\))?""".r

  private val removeGeneratedNamesRewriter = topDown(Rewriter.lift {
    case s: String => removeGeneratedNamesAndParams(s)
  })

  private val deduplicateVariableNames: String => String = fixedPoint { (in: String) =>
    val sb = new StringBuilder
    var i = 0
    for (m <- DEDUP_PATTERN.findAllMatchIn(in)) {
      sb ++= in.substring(i, m.start)
      sb ++= m.group(1)
      i = m.end
    }
    sb ++= in.substring(i)
    sb.toString()
  }

  /**
   * Removes planner-generated uniquely identifying elements from Strings.
   *
   * E.g. the String "  var@23(<uuid>)" becomes "var".
   */
  def removeGeneratedNamesAndParams(s: String): String = {
    val paramNamed = UNNAMED_PARAMS_PATTERN.replaceAllIn(s, m => s"${(m group 1).toLowerCase()}_${m group 2}")
    val named = UNNAMED_PATTERN.replaceAllIn(paramNamed, m => s"anon_${m group 2}")

    deduplicateVariableNames(named)
  }

  /**
   * Removes planner-generated uniquely identifying elements from any Strings found while traversing the tree of the given argument.
   */
  def removeGeneratedNamesAndParamsOnTree[M <: AnyRef](a: M): M = {
    removeGeneratedNamesRewriter.apply(a).asInstanceOf[M]
  }

}
