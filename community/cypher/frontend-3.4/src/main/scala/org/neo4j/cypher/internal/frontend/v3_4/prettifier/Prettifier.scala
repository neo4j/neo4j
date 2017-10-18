/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.prettifier

import org.neo4j.cypher.internal.frontend.v3_4.ast._

case class Prettifier(mkStringOf: ExpressionStringifier) {
  def asString(statement: Statement): String = statement match {
    case Query(_, SingleQuery(clauses)) =>
      clauses.map(dispatch).mkString(NL)
  }

  private def NL = {
    System.lineSeparator()
  }

  private def dispatch(clause: Clause) = clause match {
    case e: Return => asString(e)
  }

  private def asString(o: Skip): String = "SKIP " + mkStringOf(o.expression)
  private def asString(o: Limit): String = "LIMIT " + mkStringOf(o.expression)
  private def asString(o: OrderBy): String = "ORDER BY " + {
    o.sortItems.map {
      case AscSortItem(expression) => mkStringOf(expression) + " ASCENDING"
      case DescSortItem(expression) => mkStringOf(expression) + " DESCENDING"
    }.mkString(", ")
  }

  private def asString(r: ReturnItem): String = r match {
    case AliasedReturnItem(e, v) => mkStringOf(e) + " AS " + mkStringOf(v)
    case UnaliasedReturnItem(e, _) => mkStringOf(e)
  }

  private def asString(r: Return): String = {
    val d = if (r.distinct) " DISTINCT" else ""
    val i = r.returnItems.items.map(asString).mkString(", ")
    val o = r.orderBy.map(NL + "  " + asString(_)).getOrElse("")
    val l = r.limit.map(NL + "  " + asString(_)).getOrElse("")
    val s = r.skip.map(NL + "  " + asString(_)).getOrElse("")
    s"RETURN$d $i$o$s$l"
  }
}
