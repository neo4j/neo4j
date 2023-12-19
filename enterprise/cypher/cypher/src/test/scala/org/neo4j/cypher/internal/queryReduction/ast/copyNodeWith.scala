/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.queryReduction.ast

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.v3_4.expressions._

object copyNodeWith {

  trait NodeConverter {
    def ofOption[B <: ASTNode](o: Option[B]): Option[B]
    def ofSingle[B <: ASTNode](b: B): B
    def ofSeq[B <: ASTNode](bs: Seq[B]): Seq[B]
    def ofTupledSeq[B <: ASTNode, C <: ASTNode](bs: Seq[(B,C)]) : Seq[(B,C)]
  }

  def apply[A <: ASTNode, B <: ASTNode](node: A, nc: NodeConverter): A = {
    val newNode = node match {
      case Match(optional, pattern, hints, maybeWhere) =>
        Match(optional, nc.ofSingle(pattern), nc.ofSeq(hints), nc.ofOption(maybeWhere))(node.position)

      case Query(maybeHint, queryPart) =>
        Query(nc.ofOption(maybeHint), nc.ofSingle(queryPart))(node.position)

      case SingleQuery(clauses) =>
        SingleQuery(nc.ofSeq(clauses))(node.position)

      case Pattern(pParts) =>
        Pattern(nc.ofSeq(pParts))(node.position)

      case EveryPath(elem) =>
        EveryPath(nc.ofSingle(elem))

      case NodePattern(maybeVar, labels, maybeProps) =>
        NodePattern(nc.ofOption(maybeVar), nc.ofSeq(labels), nc.ofOption(maybeProps))(node.position)

      case Variable(_) => node

      case Return(distinct, returnItems, maybeGraphReturnItems, maybeOrderBy, maybeSkip, maybeLimit, excludedNames) =>
        Return(distinct,
          nc.ofSingle(returnItems),
          nc.ofOption(maybeGraphReturnItems),
          nc.ofOption(maybeOrderBy),
          nc.ofOption(maybeSkip),
          nc.ofOption(maybeLimit),
          excludedNames)(node.position)

      case ReturnItems(includeExisting, items) =>
        ReturnItems(includeExisting, nc.ofSeq(items))(node.position)

      case UnaliasedReturnItem(exp, inputText) =>
        UnaliasedReturnItem(nc.ofSingle(exp), inputText)(node.position)

      case AliasedReturnItem(exp, variable) =>
        AliasedReturnItem(nc.ofSingle(exp), nc.ofSingle(variable))(node.position)

      case Where(exp) =>
        Where(nc.ofSingle(exp))(node.position)

      case _:Literal => node

      case Parameter(_, _) => node

      case Property(map, propertyKey) =>
        Property(nc.ofSingle(map), nc.ofSingle(propertyKey))(node.position)

      case Create(pattern) =>
        Create(nc.ofSingle(pattern))(node.position)

      case HasLabels(expression, labels) =>
        HasLabels(nc.ofSingle(expression), nc.ofSeq(labels))(node.position)

      case _:SymbolicName => node

      case RelationshipChain(element, relationship, rightNode) =>
        RelationshipChain(nc.ofSingle(element), nc.ofSingle(relationship), nc.ofSingle(rightNode))(node.position)

      case RelationshipPattern(variable, types, length, properties, direction, legacyTypeSeparator) =>
        RelationshipPattern(nc.ofOption(variable), nc.ofSeq(types), Option(nc.ofOption(length.flatten)), nc.ofOption(properties), direction, legacyTypeSeparator)(node.position)

      case FunctionInvocation(namespace, functionName, distinct, args) =>
        FunctionInvocation(nc.ofSingle(namespace), nc.ofSingle(functionName), distinct, nc.ofSeq(args).toIndexedSeq)(node.position)

      case Namespace(_) => node

      case With(distinct, returnItems, mandatoryGraphReturnItems, orderBy, skip, limit, where) =>
        With(distinct, nc.ofSingle(returnItems), nc.ofSingle(mandatoryGraphReturnItems), nc.ofOption(orderBy), nc.ofOption(skip), nc.ofOption(limit), nc.ofOption(where))(node.position)

      case MapExpression(items) =>
        MapExpression(nc.ofTupledSeq(items))(node.position)

      case GraphReturnItems(includeExisting, items) =>
        GraphReturnItems(includeExisting, nc.ofSeq(items))(node.position)

      case FilterExpression(scope, expression) =>
        FilterExpression(nc.ofSingle(scope), nc.ofSingle(expression))(node.position)

      case FilterScope(variable, innerPredicate) =>
        FilterScope(nc.ofSingle(variable), nc.ofOption(innerPredicate))(node.position)

      case i:IterablePredicateExpression =>
        i.dup(Seq(nc.ofSingle(i.scope), nc.ofSingle(i.expression)))

      case ListLiteral(expressions) =>
        ListLiteral(nc.ofSeq(expressions))(node.position)

      case OrderBy(sortItems) =>
        OrderBy(nc.ofSeq(sortItems))(node.position)

      case b:BinaryOperatorExpression => b.dup(Seq(nc.ofSingle(b.lhs), nc.ofSingle(b.rhs)))

      case l:LeftUnaryOperatorExpression => l.dup(Seq(nc.ofSingle(l.rhs)))

      case r:RightUnaryOperatorExpression => r.dup(Seq(nc.ofSingle(r.lhs)))

      case s:SortItem =>
        s.dup(Seq(nc.ofSingle(s.expression)))

      case a:ASTSlicingPhrase =>
        a.dup(Seq(nc.ofSingle(a.expression)))

      case u:Union =>
        u.dup(Seq(nc.ofSingle(u.part), nc.ofSingle(u.query)))

      case CaseExpression(expression, alternatives, default) =>
        CaseExpression(nc.ofOption(expression), nc.ofTupledSeq(alternatives).toList, nc.ofOption(default))(node.position)

      case ContainerIndex(expr, idx) =>
        ContainerIndex(nc.ofSingle(expr), nc.ofSingle(idx))(node.position)

      case ReduceExpression(scope, init, list) =>
        ReduceExpression(nc.ofSingle(scope), nc.ofSingle(init), nc.ofSingle(list))(node.position)

      case ReduceScope(accumulator, variable, expression) =>
        ReduceScope(nc.ofSingle(accumulator), nc.ofSingle(variable), nc.ofSingle(expression))(node.position)

      case Foreach(variable, expression, updates) =>
        Foreach(nc.ofSingle(variable), nc.ofSingle(expression), nc.ofSeq(updates))(node.position)

      case SetClause(items) =>
        SetClause(nc.ofSeq(items))(node.position)

      case SetPropertyItem(property, expression) =>
        SetPropertyItem(nc.ofSingle(property), nc.ofSingle(expression))(node.position)

      case pc@PatternComprehension(namedPath, pattern, predicate, projection) =>
        PatternComprehension(
          nc.ofOption(namedPath),
          nc.ofSingle(pattern),
          nc.ofOption(predicate),
          nc.ofSingle(projection)
        )(node.position, nc.ofSeq(pc.outerScope.toSeq).toSet)

      case RelationshipsPattern(element) =>
        RelationshipsPattern(nc.ofSingle(element))(node.position)
    }

    newNode.asInstanceOf[A]
  }
}
