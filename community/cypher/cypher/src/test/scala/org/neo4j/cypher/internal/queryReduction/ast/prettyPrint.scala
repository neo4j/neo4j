/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.queryReduction.ast

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.v3_4.expressions._

object prettyPrint {

  def apply(node: ASTNode): String = {
    val paramString = node match {
      case Match(optional, _, _, _) =>
        s"optional=$optional"

      case Query(_, _) =>
        ""

      case SingleQuery(_) =>
        ""

      case Pattern(_) =>
        ""

      case EveryPath(_) =>
        ""

      case NodePattern(_, _, _) =>
        ""

      case Variable(name) =>
        s"name=$name"

      case Return(distinct, _, _, _, _, _, excludedNames) =>
        s"distinct=$distinct, excludedNamed=$excludedNames"

      case ReturnItems(includeExisting, _) =>
        s"includeExisting=$includeExisting"

      case UnaliasedReturnItem(_, inputText) =>
        s"inputText=$inputText"

      case AliasedReturnItem(_, _) =>
        ""

      case Where(_) =>
        ""

      case True() =>
        ""

      case Parameter(name, parameterType) =>
        s"name=$name, parameterType=$parameterType"

      case Property(_, _) =>
        ""

      case PropertyKeyName(name) =>
        s"name=$name"

      case Create(_) =>
        ""

      case And(_, _) =>
        ""

      case Equals(_, _) =>
        ""
      case HasLabels(_, _) =>
        ""

      case LabelName(name) =>
        s"name=$name"

      case RelationshipChain(_, _, _) =>
        ""

      case RelationshipPattern(_, _, _, _, direction, legacyTypeSeparator) =>
        s"direction=$direction, legacyTypeSeparator=$legacyTypeSeparator"

      case RelTypeName(name) =>
        s"name=$name"

      case FunctionInvocation(_, _, distinct, _) =>
        s"distinct=$distinct"

      case Namespace(parts) =>
        s"parts=$parts"

      case FunctionName(name) =>
        s"name=$name"

      case StringLiteral(value) =>
        s"value=$value"

      case Not(_) =>
        ""

      case With(distinct, _, _, _, _, _, _) =>
        s"distinct=$distinct"

      case MapExpression(_) =>
        ""

      case GraphReturnItems(includeExisting, _) =>
        s"includeExisting=$includeExisting"

      case FilterExpression(_, _) =>
        ""

      case FilterScope(_, _) =>
        ""

      case In(_, _) =>
        ""

      case ListLiteral(_) =>
        ""

      case SignedDecimalIntegerLiteral(stringVal) =>
        s"stringVal=$stringVal"

    }
    val childrenString = getChildren(node)
      .map(prettyPrint(_))
      .flatMap(_.split(System.lineSeparator()))
      .map("|-" + _)
      .mkString(System.lineSeparator())
    s"${node.getClass.getSimpleName}($paramString) ${System.lineSeparator()}$childrenString"

  }
}
