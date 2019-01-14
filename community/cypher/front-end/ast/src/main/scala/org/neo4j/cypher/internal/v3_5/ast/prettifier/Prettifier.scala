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
package org.neo4j.cypher.internal.v3_5.ast.prettifier

import org.neo4j.cypher.internal.v3_5.ast.{Skip, Statement, _}
import org.neo4j.cypher.internal.v3_5.expressions.{NodePattern, PatternElement, PatternPart, RelationshipChain, _}

case class Prettifier(mkStringOf: ExpressionStringifier) {
  def asString(statement: Statement): String = statement match {
    case Query(_, SingleQuery(clauses)) =>
      clauses.map(dispatch).mkString(NL)
  }

  private def dispatch(clause: Clause) = clause match {
    case e: Return => asString(e)
    case m: Match => asString(m)
    case w: With => asString(w)
    case c: Create => asString(c)
    case u: Unwind => asString(u)
    case u: UnresolvedCall => asString(u)
    case s: SetClause => asString(s)
    case d: Delete => asString(d)
    case m: Merge => asString(m)
    case _ => clause.asCanonicalStringVal // TODO
  }

  private def NL = System.lineSeparator()

  def asString(element: PatternElement): String = element match {
    case r: RelationshipChain => mkStringOf.pattern(r)
    case n: NodePattern => mkStringOf.node(n)
  }

  def asString(p: PatternPart): String = p match {
    case EveryPath(element) => asString(element)
    case NamedPatternPart(variable, patternPart) => s"${mkStringOf(variable)} = ${asString(patternPart)}"
    case ShortestPaths(pattern, single) =>
      val name = if(single) "shortestPath" else "shortestPaths"
      s"$name(${asString(pattern)})"
  }

  def asString(m: Match): String = {
    val o = if(m.optional) "OPTIONAL " else ""
    val p = m.pattern.patternParts.map(p => asString(p)).mkString(", ")
    val w = m.where.map(w => NL + "  WHERE " + mkStringOf(w.expression)).getOrElse("")
    s"${o}MATCH $p$w"
  }

  private def asString(merge: Merge): String = {
    s"MERGE ${merge.pattern.patternParts.map(asString).mkString(", ")}"
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

  private def asString(w: With): String = {
    val d = if (w.distinct) " DISTINCT" else ""
    val i = w.returnItems.items.map(asString).mkString(", ")
    val o = w.orderBy.map(NL + "  " + asString(_)).getOrElse("")
    val l = w.limit.map(NL + "  " + asString(_)).getOrElse("")
    val s = w.skip.map(NL + "  " + asString(_)).getOrElse("")
    val wh = w.where.map(w => NL + "  WHERE " + mkStringOf(w.expression)).getOrElse("")
    s"WITH$d $i$o$s$l$wh"
  }

  private def asString(c: Create): String = {
    val p = c.pattern.patternParts.map(p => asString(p)).mkString(", ")
    s"CREATE $p"
  }

  private def asString(u: Unwind): String = {
    s"UNWIND ${mkStringOf(u.expression)} AS ${mkStringOf(u.variable)}"
  }

  private def asString(u: UnresolvedCall): String = {
    val namespace = u.procedureNamespace.parts.mkString(".")
    val prefix = if (namespace.isEmpty) "" else namespace + "."
    val arguments = u.declaredArguments.map(list => list.map(mkStringOf(_)).mkString(", ")).getOrElse("")
    val yields = u.declaredResult.map(result => " YIELD " + result.items.map(item => mkStringOf(item.variable)).mkString(", ")).getOrElse("")
    s"CALL $prefix${u.procedureName.name}($arguments)$yields"
  }

  private def asString(s: SetClause): String = {
    val items = s.items.map {
      case SetPropertyItem(prop, exp) => s"${mkStringOf(prop)} = ${mkStringOf(exp)}"
      case SetLabelItem(variable, labels) => mkStringOf(variable) + labels.map(label =>s":${ExpressionStringifier.backtick(label.name)}").mkString("")
      case SetIncludingPropertiesFromMapItem(variable, exp) => s"${mkStringOf(variable)} += ${mkStringOf(exp)}"
      case SetExactPropertiesFromMapItem(variable, exp) => s"${mkStringOf(variable)} = ${mkStringOf(exp)}"
      case _ => s.asCanonicalStringVal
    }
    s"SET ${items.mkString(", ")}"
  }

  private def asString(delete: Delete): String = {
    s"DELETE ${delete.expressions.map(mkStringOf(_)).mkString(", ")}"
  }
}
