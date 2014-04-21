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
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_1.{Rewriter, topDown}

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

      val (patternNodes, relationships, namedPaths) = PatternDestructuring.destruct(rewrittenChain)
      val qg = QueryGraph(
        patternRelationships = relationships.toSet,
        patternNodes = patternNodes.toSet,
        namedPaths = namedPaths.toSet
      ).add(predicates)
      qg.addArgumentId(qg.coveredIds.toSeq)
    }
  }

  object PatternDestructuring {
    def destruct(pattern: Pattern): (Seq[IdName], Seq[PatternRelationship], Seq[NamedPath]) =
      pattern.patternParts.foldLeft((Seq.empty[IdName], Seq.empty[PatternRelationship], Seq.empty[NamedPath])) {
        case ((accIdNames, accRels, accPaths), everyPath: EveryPath) =>
          val (idNames, rels, paths) = destruct(everyPath.element)
          (accIdNames ++ idNames, accRels ++ rels, accPaths ++ paths)

        case ((accIdNames, accRels, accPaths), NamedPatternPart(Identifier(name), everyPath@EveryPath(_))) =>
          val (idNames, rels, paths) = destruct(everyPath.element)
          val namedPath = if (rels.isEmpty)
          // EveryPath contains a NodePattern (having only one idName)
            NamedNodePath(IdName(name), idNames.head)
          else
          // EveryPath contains a RelationshipChain
            NamedRelPath(IdName(name), rels)

          (accIdNames ++ idNames, accRels ++ rels, accPaths ++ Seq(namedPath) ++ paths)

        case _ => throw new CantHandleQueryException
      }

    private def destruct(element: PatternElement): (Seq[IdName], Seq[PatternRelationship], Seq[NamedPath]) = element match {
      case relchain: RelationshipChain => destruct(relchain)
      case node: NodePattern => destruct(node)
    }

    def destruct(chain: RelationshipChain): (Seq[IdName], Seq[PatternRelationship], Seq[NamedPath]) = chain match {
      // (a)->[r]->(b)
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None, _), RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val leftNode = IdName(leftNodeId.name)
        val rightNode = IdName(rightNodeId.name)
        val r = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, asPatternLength(length))
        (Seq(leftNode, rightNode), Seq(r), Seq.empty)

      // ...->[r]->(b)
      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val (idNames, rels, paths) = destruct(relChain)
        val leftNode = IdName(rels.last.nodes._2.name)
        val rightNode = IdName(rightNodeId.name)
        val resultRels = rels :+ PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, asPatternLength(length))
        (idNames :+ rightNode, resultRels, paths)

      case _ => throw new CantHandleQueryException
    }

    private def destruct(node: NodePattern): (Seq[IdName], Seq[PatternRelationship], Seq[NamedPath]) =
      (Seq(IdName(node.identifier.get.name)), Seq.empty, Seq.empty)

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

  private def getSelectionsAndSubQueries(optWhere: Option[Where]): (Selections, Seq[SubQuery]) = {
    import SimpleQueryGraphBuilder.SubQueryExtraction.extractQueryGraph

    val predicates: Set[Predicate] = optWhere.map(SelectionPredicates.fromWhere).getOrElse(Set.empty)
    val subQueries: Seq[SubQuery] = predicates.collect {
      case Predicate(_, exp: PatternExpression) =>
        Exists(exp, extractQueryGraph(exp))
    }.toSeq

    (Selections(predicates), subQueries)
  }

  override def produce(ast: Query): QueryGraph = ast match {
    case Query(None, SingleQuery(clauses)) =>
      clauses.foldLeft(QueryGraph.empty)(
        (qg, clause) =>
          clause match {
            case Return(false, ListedReturnItems(expressions), None, None, None) =>
              val projections: Seq[(String, Expression)] = expressions.map(e => e.name -> e.expression)
              if (projections.exists {
                case (_,e) => e.asCommandExpression.containsAggregate
              }) throw new CantHandleQueryException

              qg.changeProjections(projections.toMap)

            case Match(optional@false, pattern: Pattern, Seq(), optWhere) =>
              if (qg.patternRelationships.nonEmpty || qg.patternNodes.nonEmpty)
                throw new CantHandleQueryException

              val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)

              val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship], namedPaths: Seq[NamedPath]) = destruct(pattern)
              val matchClause = QueryGraph(
                selections = selections,
                patternNodes = nodeIds.toSet,
                patternRelationships = rels.toSet,
                namedPaths = namedPaths.toSet,
                subQueries = subQueries)

              qg ++ matchClause

            case Match(optional@true, pattern: Pattern, Seq(), optWhere) =>
              val (nodeIds: Seq[IdName], rels: Seq[PatternRelationship], namedPaths: Seq[NamedPath]) = destruct(pattern)
              val (selections, subQueries) = getSelectionsAndSubQueries(optWhere)
              val optionalMatch = QueryGraph(
                selections = selections,
                patternNodes = nodeIds.toSet,
                namedPaths = namedPaths.toSet,
                subQueries = subQueries,
                patternRelationships = rels.toSet).addCoveredIdsAsProjections()

              qg.withAddedOptionalMatch(optionalMatch)

            case _ =>
              throw new CantHandleQueryException
          }
      )

    case _ =>
      throw new CantHandleQueryException
  }

}
