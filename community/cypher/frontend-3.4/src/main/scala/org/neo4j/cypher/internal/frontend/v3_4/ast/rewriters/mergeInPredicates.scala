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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.Where
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions._

/**
  * Merges multiple IN predicates into one.
  *
  * For example, MATCH (n) WHERE n.prop IN [1,2,3] AND [2,3,4] RETURN n.prop
  * will be rewritten into MATCH (n) WHERE n.prop IN [2,3]
  *
  * NOTE: this rewriter must be applied before auto parameterization, since after
  * that we are just dealing with parameters.
  */
case object mergeInPredicates extends Rewriter {

  def apply(that: AnyRef): AnyRef = inner.apply(that)

  private val inner: Rewriter = bottomUp(Rewriter.lift {
    case where@Where(e) =>
      val rewrittenPredicates = e.endoRewrite(bottomUp(Rewriter.lift {
        case and@And(lhs, rhs) =>
          val rewriter = inRewriter(collectInPredicates(lhs, rhs))
          val newLhs = lhs.endoRewrite(rewriter)
          val newRhs = rhs.endoRewrite(rewriter)
          if (newLhs == newRhs) newLhs
          else and.copy(lhs = newLhs, rhs = newRhs)(and.position)
      }))

      where.copy(expression = rewrittenPredicates)(where.position)
  })

  private def inRewriter(map: Map[Expression, Seq[Expression]]) = bottomUp(Rewriter.lift({
    case in@In(a, l@ListLiteral(_)) =>
      val expressions = map(a)
      if (expressions.nonEmpty) in.copy(rhs = l.copy(map(a))(l.position))(in.position)
      else False()(in.position)
  }))

  //Collect all a IN [..] as a map, with {a -> [...], ...}
  private def collectInPredicates(expressions: Expression*): Map[Expression, Seq[Expression]] = {
    val maps = expressions.map(_.treeFold(Map.empty[Expression, Seq[Expression]]) {
      case In(a, ListLiteral(exprs)) => (map) => {
        val values = map.get(a).map(current => current intersect exprs).getOrElse(exprs).distinct
        (map + (a -> values), None)
      }
    })
    maps.reduceLeft((acc, current) => {
      val sharedKeys = acc.keySet intersect current.keySet
      val updates = sharedKeys.map(k => k -> (acc(k) intersect current(k)).distinct)
      acc ++ updates ++ (current -- sharedKeys)
    })
  }
}

//Actual   :Query(None,SingleQuery(List(Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(a)),List(),None)))),List(),Some(Where(
//
// And(
//  And(
//    And(
//      In(Property(Variable(a),PropertyKeyName(prop)),ListLiteral(List(SignedDecimalIntegerLiteral(2), SignedDecimalIntegerLiteral(3)))),
//      In(Property(Variable(a),PropertyKeyName(foo)),ListLiteral(List(StringLiteral(bar))))),
//    In(Property(Variable(a),PropertyKeyName(prop)),ListLiteral(List(SignedDecimalIntegerLiteral(2), SignedDecimalIntegerLiteral(3))))),
// In(Property(Variable(a),PropertyKeyName(foo)),ListLiteral(List(StringLiteral(bar)))))))), Return(false,ReturnItems(true,List()),None,None,None,None,Set()))))
