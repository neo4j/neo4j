/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions.{FunctionInvocation, FunctionName}

import scala.collection.immutable.TreeMap

case object replaceAliasedFunctionInvocations extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  /*
   * These are historical names for functions. They are all subject to removal in an upcoming major release.
   */
  val aliases: Map[String, String] = TreeMap("toInt" -> "toInteger",
                                             "upper" -> "toUpper",
                                             "lower" -> "toLower",
                                             "rels" -> "relationships")(CaseInsensitiveOrdered)

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case func@FunctionInvocation(_, f@FunctionName(name), _, _) if aliases.get(name).nonEmpty =>
      func.copy(functionName = FunctionName(aliases(name))(f.position))(func.position)
  })

}

object CaseInsensitiveOrdered extends Ordering[String] {
  def compare(x: String, y: String): Int =
    x.compareToIgnoreCase(y)
}
