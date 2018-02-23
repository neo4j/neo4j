/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.queryReduction.ast

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.v3_4.expressions.{BinaryOperatorExpression, _}

object getChildren {

  private def ofOption[I](maybe: Option[I]): Seq[I] = {
    maybe match {
      case None => Seq()
      case Some(thing) => Seq(thing)
    }
  }

  def apply(node: ASTNode): Seq[ASTNode] = {
    node match {
      case Match(_, pattern, hints, maybeWhere) =>
        Seq(pattern) ++ hints ++ ofOption(maybeWhere)

      case Query(maybeHint, queryPart) =>
        ofOption(maybeHint) ++ Seq(queryPart)

      case SingleQuery(clauses) =>
        clauses

      case Pattern(pParts) =>
        pParts

      case EveryPath(elem) =>
        Seq(elem)

      case NodePattern(maybeVar, labels, maybeProps) =>
        ofOption(maybeVar) ++ labels ++ ofOption(maybeProps)

      case Variable(_) =>
        Seq()

      case Return(_, returnItems, maybeGraphReturnItems, maybeOrderBy, maybeSkip, maybeLimit, _) =>
        Seq(returnItems) ++ ofOption(maybeGraphReturnItems) ++ ofOption(maybeOrderBy) ++
          ofOption(maybeSkip) ++ ofOption(maybeLimit)

      case ReturnItems(_, items) =>
        items

      case UnaliasedReturnItem(exp, _) =>
        Seq(exp)

      case AliasedReturnItem(exp, variable) =>
        Seq(exp, variable)

      case Where(exp) =>
        Seq(exp)

      case _:Literal =>
        Seq()

      case Parameter(_, _) =>
        Seq()

      case Property(map, propertyKey) =>
        Seq(map, propertyKey)

      case Create(pattern) =>
        Seq(pattern)

      case HasLabels(expression, labels) =>
        Seq(expression) ++ labels

      case _:SymbolicName =>
        Seq()

      case RelationshipChain(element, relationship, rightNode) =>
        Seq(element, relationship, rightNode)

      case RelationshipPattern(variable, types, length, properties, _, _) =>
        ofOption(variable) ++  types ++ ofOption(length.flatten) ++ ofOption(properties)

      case FunctionInvocation(namespace, functionName, _, args) =>
        Seq(namespace, functionName) ++ args

      case Namespace(_) =>
        Seq()

      case With(distinct, returnItems, mandatoryGraphReturnItems, orderBy, skip, limit, where) =>
        Seq(returnItems, mandatoryGraphReturnItems) ++
        ofOption(orderBy) ++ ofOption(skip) ++ ofOption(limit) ++ ofOption(where)

      case MapExpression(items) =>
        items.flatMap { case (pkn, exp) => Seq(pkn, exp) }

      case GraphReturnItems(_, items) =>
        items

      case FilterExpression(scope, expression) =>
        Seq(scope, expression)

      case i:IterablePredicateExpression =>
        Seq(i.scope, i.expression)

      case FilterScope(variable, innerPredicate) =>
        Seq(variable) ++ ofOption(innerPredicate)

      case ListLiteral(expressions) =>
        expressions

      case OrderBy(sortItems) =>
        sortItems

      case b:BinaryOperatorExpression =>
        Seq(b.lhs, b.rhs)

      case l:LeftUnaryOperatorExpression =>
        Seq(l.rhs)

      case r:RightUnaryOperatorExpression =>
        Seq(r.lhs)

      case m:MultiOperatorExpression =>
        m.exprs.toSeq

      case s:SortItem =>
        Seq(s.expression)

      case a:ASTSlicingPhrase =>
        Seq(a.expression)

      case u:Union =>
        Seq(u.part, u.query)

      case CaseExpression(expression, alternatives, default) =>
        ofOption(expression) ++ alternatives.flatMap { case (a, b) => Seq(a, b) } ++ ofOption(default)

      case ContainerIndex(expr, idx) =>
        Seq(expr, idx)

      case ReduceExpression(scope, init, list) =>
        Seq(scope, init, list)

      case ReduceScope(accumulator, variable, expression) =>
        Seq(accumulator, variable, expression)

      case Foreach(variable, expression, updates) =>
        Seq(variable, expression) ++ updates

      case SetClause(items) =>
        items

      case SetPropertyItem(property, expression) =>
        Seq(property, expression)

      case PatternComprehension(namedPath, pattern, predicate, projection, outerScope) =>
        ofOption(namedPath) ++ Seq(pattern) ++ ofOption(predicate) ++ Seq(projection) ++ outerScope.toSeq

      case RelationshipsPattern(element) =>
        Seq(element)
    }
  }

}
