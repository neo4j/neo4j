/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

      case _:Literal => Seq()

      case Parameter(_, _) => Seq()

      case Property(map, propertyKey) =>
        ofSingle(map, classOf[Expression]) ++
          ofSingle(propertyKey, classOf[PropertyKeyName])

      case Create(pattern) =>
        ofSingle(pattern, classOf[Pattern])

      case HasLabels(expression, labels) =>
        ofSingle(expression, classOf[Expression]) ++
          ofSeq(labels, classOf[LabelName])

      case _:SymbolicName => Seq()

      case RelationshipChain(element, relationship, rightNode) =>
        ofSingle(element, classOf[PatternElement]) ++
          ofSingle(relationship, classOf[RelationshipPattern]) ++
          ofSingle(rightNode, classOf[NodePattern])

      case RelationshipPattern(variable, types, length, properties, _, _) =>
        ofOption(variable, classOf[Variable]) ++
          ofSeq(types, classOf[RelTypeName]) ++
          ofOption(length.flatten, classOf[Range]) ++
          ofOption(properties, classOf[Expression])

      case FunctionInvocation(namespace, functionName, distinct, args) =>
        ofSingle(namespace, classOf[Namespace]) ++
          ofSingle(functionName, classOf[FunctionName]) ++
          ofSeq(args, classOf[Expression])

      case Namespace(_) => Seq()

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

      case i:IterablePredicateExpression =>
        ofSingle(i.scope, classOf[FilterScope]) ++
        ofSingle(i.expression, classOf[Expression])

      case ListLiteral(expressions) =>
        ofSeq(expressions, classOf[Expression])

      case OrderBy(sortItems) =>
        ofSeq(sortItems, classOf[SortItem])

      case b:BinaryOperatorExpression =>
        ofSingle(b.lhs, classOf[Expression]) ++
          ofSingle(b.rhs, classOf[Expression])

      case l:LeftUnaryOperatorExpression =>
        ofSingle(l.rhs, classOf[Expression])

      case r:RightUnaryOperatorExpression =>
        ofSingle(r.lhs, classOf[Expression])

      case m:MultiOperatorExpression =>
        ofSeq(m.exprs.toSeq, classOf[Expression])

      case s:SortItem =>
        ofSingle(s.expression, classOf[Expression])

      case a:ASTSlicingPhrase =>
        ofSingle(a.expression, classOf[Expression])

      case u:Union =>
        ofSingle(u.part, classOf[QueryPart]) ++
        ofSingle(u.query, classOf[SingleQuery])

      case CaseExpression(expression, alternatives, default) =>
        ofOption(expression, classOf[Expression]) ++
        ofTupledSeq(alternatives, classOf[Expression], classOf[Expression]) ++
        ofOption(default, classOf[Expression])

      case ContainerIndex(expr, idx) =>
        ofSingle(expr, classOf[Expression]) ++
        ofSingle(idx, classOf[Expression])

      case ReduceExpression(scope, init, list) =>
        ofSingle(scope, classOf[ReduceScope]) ++
        ofSingle(init, classOf[Expression]) ++
        ofSingle(list, classOf[Expression])

      case ReduceScope(accumulator, variable, expression) =>
        ofSingle(accumulator, classOf[Variable]) ++
        ofSingle(variable, classOf[Variable]) ++
        ofSingle(expression, classOf[Expression])

      case Foreach(variable, expression, updates) =>
        ofSingle(variable, classOf[Variable]) ++
        ofSingle(expression, classOf[Expression]) ++
        ofSeq(updates, classOf[Clause])

      case SetClause(items) =>
        ofSeq(items,classOf[SetItem])

      case SetPropertyItem(property, expression) =>
        ofSingle(property, classOf[Property]) ++
        ofSingle(expression, classOf[Expression])

      case PatternComprehension(namedPath, pattern, predicate, projection) =>
        ofOption(namedPath, classOf[LogicalVariable]) ++
          ofSingle(pattern, classOf[RelationshipsPattern]) ++
          ofOption(predicate, classOf[Expression]) ++
          ofSingle(projection, classOf[Expression])

      case RelationshipsPattern(element) =>
        ofSingle(element, classOf[RelationshipChain])
    }
  }
}
