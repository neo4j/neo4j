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

object domainsOf {

  def apply[T](grandParent: ASTNode)(makeDomain: (ASTNode, Class[_]) => T): Seq[T] = {
    def ofOption(o: Option[ASTNode], expectedType: Class[_]): Seq[T] = {
      o.map(makeDomain(_, expectedType)).toSeq
    }

    def ofSeq(bs: Seq[ASTNode], expectedType: Class[_]): Seq[T] = {
      bs.map(makeDomain(_, expectedType))
    }

    def ofSingle(node: ASTNode, expectedType: Class[_]): Seq[T] = {
      Seq(makeDomain(node, expectedType))
    }

    def ofTupledSeq(bs: Seq[(ASTNode,ASTNode)], expectedFirstType: Class[_], expectedSecondType: Class[_]) : Seq[T] = {
      bs.flatMap { case (b,c) => Seq(makeDomain(b, expectedFirstType), makeDomain(c, expectedSecondType))}
    }

    grandParent match {
      case Match(_, pattern, hints, maybeWhere) =>
        ofSingle(pattern, classOf[Pattern]) ++ ofSeq(hints, classOf[UsingHint]) ++ ofOption(maybeWhere, classOf[Where])

      case Query(maybeHint, queryPart) =>
        ofOption(maybeHint, classOf[PeriodicCommitHint]) ++ ofSingle(queryPart, classOf[QueryPart])

      case SingleQuery(clauses) =>
        ofSeq(clauses, classOf[Clause])

      case Pattern(pParts) =>
        ofSeq(pParts, classOf[PatternPart])

      case EveryPath(elem) =>
        ofSingle(elem, classOf[PatternElement])

      case NodePattern(maybeVar, labels, maybeProps) =>
        ofOption(maybeVar, classOf[Variable]) ++ ofSeq(labels, classOf[LabelName]) ++ ofOption(maybeProps, classOf[Expression])

      case Variable(_) => Seq()

      case Return(_, returnItems, maybeGraphReturnItems, maybeOrderBy, maybeSkip, maybeLimit, _) =>
        ofSingle(returnItems, classOf[ReturnItemsDef]) ++
          ofOption(maybeGraphReturnItems, classOf[GraphReturnItems]) ++
          ofOption(maybeOrderBy, classOf[OrderBy]) ++
          ofOption(maybeSkip, classOf[Skip]) ++
          ofOption(maybeLimit, classOf[Limit])

      case ReturnItems(_, items) =>
        ofSeq(items, classOf[ReturnItem])

      case UnaliasedReturnItem(exp, _) =>
        ofSingle(exp, classOf[Expression])

      case AliasedReturnItem(exp, variable) =>
        ofSingle(exp, classOf[Expression]) ++
          ofSingle(variable, classOf[Variable])

      case Where(exp) =>
        ofSingle(exp, classOf[Expression])

      case True() => Seq()

      case Parameter(_, _) => Seq()

      case Property(map, propertyKey) =>
        ofSingle(map, classOf[Expression]) ++
          ofSingle(propertyKey, classOf[PropertyKeyName])

      case PropertyKeyName(_) => Seq()

      case Create(pattern) =>
        ofSingle(pattern, classOf[Pattern])

      case And(lhs, rhs) =>
        ofSingle(lhs, classOf[Expression]) ++
          ofSingle(rhs, classOf[Expression])

      case Equals(lhs, rhs) =>
        ofSingle(lhs, classOf[Expression]) ++
          ofSingle(rhs, classOf[Expression])

      case HasLabels(expression, labels) =>
        ofSingle(expression, classOf[Expression]) ++
          ofSeq(labels, classOf[LabelName])

      case LabelName(_) => Seq()

      case RelationshipChain(element, relationship, rightNode) =>
        ofSingle(element, classOf[PatternElement]) ++
          ofSingle(relationship, classOf[RelationshipPattern]) ++
          ofSingle(rightNode, classOf[NodePattern])

      case RelationshipPattern(variable, types, length, properties, _, _) =>
        ofOption(variable, classOf[Variable]) ++
          ofSeq(types, classOf[RelTypeName]) ++
          ofOption(length.flatten, classOf[Range]) ++
          ofOption(properties, classOf[Expression])

      case RelTypeName(_) => Seq()

      case FunctionInvocation(namespace, functionName, distinct, args) =>
        ofSingle(namespace, classOf[Namespace]) ++
          ofSingle(functionName, classOf[FunctionName]) ++
          ofSeq(args, classOf[Expression])

      case Namespace(_) => Seq()

      case FunctionName(_) => Seq()

      case StringLiteral(_) => Seq()

      case Not(rhs) =>
        ofSingle(rhs, classOf[Expression])

      case With(_, returnItems, mandatoryGraphReturnItems, orderBy, skip, limit, where) =>
        ofSingle(returnItems, classOf[ReturnItemsDef]) ++
        ofSingle(mandatoryGraphReturnItems, classOf[GraphReturnItems]) ++
        ofOption(orderBy, classOf[OrderBy]) ++
        ofOption(skip, classOf[Skip]) ++
        ofOption(limit, classOf[Limit]) ++
        ofOption(where, classOf[Where])

      case MapExpression(items) =>
        ofTupledSeq(items, classOf[PropertyKeyName], classOf[Expression])

      case GraphReturnItems(_, items) =>
        ofSeq(items, classOf[GraphReturnItem])

      case FilterExpression(scope, expression) =>
        ofSingle(scope, classOf[FilterScope]) ++
        ofSingle(expression, classOf[Expression])

      case FilterScope(variable, innerPredicate) =>
        ofSingle(variable, classOf[Variable]) ++
        ofOption(innerPredicate, classOf[Expression])

      case In(lhs, rhs) =>
        ofSingle(lhs, classOf[Expression]) ++
        ofSingle(rhs, classOf[Expression])

      case ListLiteral(expressions) =>
        ofSeq(expressions, classOf[Expression])

      case SignedDecimalIntegerLiteral(_) => Seq()
    }
  }
}
