/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_1.{Rewriter, topDown}
import org.neo4j.cypher.internal.compiler.v2_1.helpers.UnNamedNameGenerator._

object SimpleQueryGraphBuilder {

  object SubQueryExtraction {

    val normalizer = MatchPredicateNormalizerChain(PropertyPredicateNormalizer, LabelPredicateNormalizer)

    def extractQueryGraph(exp: PatternExpression): QueryGraph = {
      val relChain: RelationshipChain = exp.pattern.element
      val predicates: Vector[Expression] = relChain.fold(Vector.empty[Expression]) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _ => identity
      }

      val rewrittenChain = relChain.rewrite(topDown(Rewriter.lift(normalizer.replace))).asInstanceOf[RelationshipChain]

      val (patternNodes, relationships) = PatternDestructuring.destruct(rewrittenChain)
      val qg = QueryGraph(
        patternRelationships = relationships.toSet,
        patternNodes = patternNodes.toSet
      ).addPredicates(predicates).addCoveredIdsAsProjections()
      qg.copy(argumentIds = qg.coveredIds.filter(_.name.isNamed))
    }
  }

  object PatternDestructuring {
    def destruct(pattern: Pattern): (Seq[IdName], Seq[PatternRelationship]) =
      pattern.patternParts.foldLeft((Seq.empty[IdName], Seq.empty[PatternRelationship])) {
        case ((accIdNames, accRels), everyPath: EveryPath) =>
          val (idNames, rels) = destruct(everyPath.element)
          (accIdNames ++ idNames, accRels ++ rels)

        case _ => throw new CantHandleQueryException
      }

    private def destruct(element: PatternElement): (Seq[IdName], Seq[PatternRelationship]) = element match {
      case relchain: RelationshipChain => destruct(relchain)
      case node: NodePattern => destruct(node)
    }

    def destruct(chain: RelationshipChain): (Seq[IdName], Seq[PatternRelationship]) = chain match {
      // (a)->[r]->(b)
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None, _), RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val leftNode = IdName(leftNodeId.name)
        val rightNode = IdName(rightNodeId.name)
        val r = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, asPatternLength(length))
        (Seq(leftNode, rightNode), Seq(r))

      // ...->[r]->(b)
      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val (idNames, rels) = destruct(relChain)
        val leftNode = IdName(rels.last.right.name)
        val rightNode = IdName(rightNodeId.name)
        val resultRels = rels :+ PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, asPatternLength(length))
        (idNames :+ rightNode, resultRels)

      case _ => throw new CantHandleQueryException
    }

    private def destruct(node: NodePattern): (Seq[IdName], Seq[PatternRelationship]) =
      (Seq(IdName(node.identifier.get.name)), Seq.empty)

    private def asPatternLength(length: Option[Option[Range]]): PatternLength = length match {
      case Some(Some(Range(Some(left), Some(right)))) => VarPatternLength(left.value.toInt, Some(right.value.toInt))
      case Some(Some(Range(Some(left), None))) => VarPatternLength(left.value.toInt, None)
      case Some(Some(Range(None, Some(right)))) => VarPatternLength(1, Some(right.value.toInt))
      case Some(Some(Range(None, None))) => VarPatternLength.unlimited
      case Some(None) => VarPatternLength.unlimited
      case None => SimplePatternLength
    }
  }

}

class SimpleQueryGraphBuilder extends QueryGraphBuilder {
  import SimpleQueryGraphBuilder.PatternDestructuring._

  private def getSelectionsAndSubQueries(optWhere: Option[Where]): (Selections, Seq[(PatternExpression, QueryGraph)]) = {
    import SimpleQueryGraphBuilder.SubQueryExtraction.extractQueryGraph

    val predicates: Set[Predicate] = optWhere.map(SelectionPredicates.fromWhere).getOrElse(Set.empty)

    val predicatesWithCorrectDeps = predicates.map {
      case Predicate(deps, e: PatternExpression) =>
        Predicate(deps.filter(x => isNamed(x.name)), e)
      case Predicate(deps, ors@Ors(exprs)) =>
        val newDeps = exprs.foldLeft(Set.empty[IdName]) { (acc, exp) =>
          exp match {
            case exp: PatternExpression => acc ++ SelectionPredicates.idNames(exp).filter(x => isNamed(x.name))
            case _                      => acc ++ SelectionPredicates.idNames(exp)
          }
        }
        Predicate(newDeps, ors)
      case p => p
    }

    val subQueries = predicates.collect {
      case Predicate(_, Ors((_:PatternExpression) :: (_:PatternExpression) :: _ )) =>
        throw new CantHandleQueryException
      case Predicate(_, Ors(Not(_:PatternExpression) :: (_:PatternExpression) :: _ )) =>
        throw new CantHandleQueryException
      case Predicate(_, Ors((_:PatternExpression) :: Not(_:PatternExpression) :: _ )) =>
        throw new CantHandleQueryException
      case Predicate(_, Ors(Not(_:PatternExpression) :: Not(_:PatternExpression) :: _ )) =>
        throw new CantHandleQueryException

      case Predicate(_, Ors((patternExpr:PatternExpression) :: tail)) if !tail.exists(_.isInstanceOf[PatternExpression])  =>
        (patternExpr, extractQueryGraph(patternExpr))

      case Predicate(_, Ors((Not(patternExpr: PatternExpression)) :: tail)) if !tail.exists(_.isInstanceOf[PatternExpression])  =>
        (patternExpr, extractQueryGraph(patternExpr))

      case Predicate(_, Not(patternExpr: PatternExpression)) =>
        (patternExpr, extractQueryGraph(patternExpr))

      case Predicate(_, patternExpr: PatternExpression) =>
        (patternExpr, extractQueryGraph(patternExpr))

    }.toSeq

    (Selections(predicates), subQueries)
  }

  override def produce(ast: Query): QueryGraph = ast match {
    case Query(None, SingleQuery(clauses)) =>
      produceQueryGraphFromClauses(QueryGraph.empty, clauses)

    case _ =>
      throw new CantHandleQueryException
  }

  private def produceQueryGraphFromClauses(qg: QueryGraph, clauses: Seq[Clause]): QueryGraph =
      clauses match {
        case Return(false, ListedReturnItems(expressions), optOrderBy, skip, limit) :: tl =>
          val (projections, aggregations) = produceProjectionsMap(expressions)
          val sortItems = produceSortItems(optOrderBy)

          val newQG = qg
            .withSortItems(sortItems)
            .withProjections(projections)
            .withAggregatingProjections(aggregations)
            .copy(
              limit = limit.map(_.expression),
              skip = skip.map(_.expression)
            )

          produceQueryGraphFromClauses(newQG, tl)

        case Match(optional@false, pattern: Pattern, Seq(), optWhere) :: tl =>
          val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)

          val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship]) = destruct(pattern)
          val matchClause = QueryGraph(
            selections = selections,
            patternNodes = nodeIds.toSet,
            patternRelationships = rels.toSet,
            subQueriesLookupTable = subQueries.toMap)

          val newQG = qg ++ matchClause

          produceQueryGraphFromClauses(newQG, tl)

        case Match(optional@true, pattern: Pattern, Seq(), optWhere) :: tl =>
          val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship]) = destruct(pattern)
          val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
          val optionalMatch = QueryGraph(
            selections = selections,
            patternNodes = nodeIds.toSet,
            subQueriesLookupTable = subQueries.toMap,
            patternRelationships = rels.toSet).addCoveredIdsAsProjections()


          val newQG = qg.withAddedOptionalMatch(optionalMatch)

          produceQueryGraphFromClauses(newQG, tl)

        case With(false, _: ReturnAll, optOrderBy, None, None, optWhere) :: tl =>
          val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)

          val newQG: QueryGraph = QueryGraph(
            sortItems = produceSortItems(optOrderBy),
            selections = selections,
            subQueriesLookupTable = subQueries.toMap
          )

          produceQueryGraphFromClauses(qg ++ newQG, tl)

        case With(false, ListedReturnItems(expressions), optOrderBy, skip, limit, optWhere) :: tl =>
          val (projections, aggregations) = produceProjectionsMap(expressions)
          val sortItems = produceSortItems(optOrderBy)

          val newQG: QueryGraph = qg
            .withSortItems(sortItems)
            .withProjections(projections)
            .withAggregatingProjections(aggregations)
            .copy(
              limit = limit.map(_.expression),
              skip = skip.map(_.expression)
            )

          val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)

          val tail = produceQueryGraphFromClauses(QueryGraph(
            selections = selections,
            subQueriesLookupTable = subQueries.toMap
          ), tl)
          newQG.withTail(tail)

        case Seq() =>
          qg

        case _ =>
          throw new CantHandleQueryException
      }

  private def produceSortItems(optOrderBy: Option[OrderBy]) = optOrderBy.fold(Seq.empty[SortItem])(_.sortItems)

  private def produceProjectionsMap(expressions: Seq[ReturnItem]): (Map[String, Expression], Map[String, Expression]) = {
    val (projections, aggregations) = expressions.foldLeft((Map.empty[String, Expression], Map.empty[String, Expression])) {
      case ((projections, aggregations), item) =>
        val expr = item.expression
        val pair = item.name -> expr
        if (containsAggregate(expr))
          (projections, aggregations + pair)
        else
          (projections + pair, aggregations)
    }
    (projections.toMap, aggregations.toMap)
  }
}
