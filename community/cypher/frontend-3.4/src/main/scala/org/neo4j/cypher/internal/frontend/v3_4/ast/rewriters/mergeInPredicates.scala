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
  * Examples:
  * MATCH (n) WHERE n.prop IN [1,2,3] AND [2,3,4] RETURN n.prop
  * => MATCH (n) WHERE n.prop IN [2,3]
  *
  * MATCH (n) WHERE n.prop IN [1,2,3] OR [2,3,4] RETURN n.prop
  * => MATCH (n) WHERE n.prop IN [1,2,3,4]
  *
  * MATCH (n) WHERE n.prop IN [1,2,3] AND [4,5,6] RETURN n.prop
  * => MATCH (n) WHERE FALSE
  *
  * NOTE: this rewriter must be applied before auto parameterization, since after
  * that we are just dealing with opaque parameters.
  */
case object mergeInPredicates extends Rewriter {

  def apply(that: AnyRef): AnyRef = inner.apply(that)

  private val inner: Rewriter = bottomUp(Rewriter.lift {
    //Look for a `IN [...] AND a IN [...]` and compute the intersection of lists
    case and@And(lhs, rhs) =>
      rewriteBinaryOperator(and, (a, b) => a intersect b, (l, r) => and.copy(l, r)(and.position))
    //Look for `a IN [...] OR a IN [...]` and compute union of lists
    case or@Or(lhs, rhs) =>
      rewriteBinaryOperator(or, (a, b) => a union b,
                            (l, r) => or.copy(l, r)(or.position))
  })

  //Takes a binary operator a merge operator and a copy constructor
  //and rewrites the binary operator
  private def rewriteBinaryOperator(binary: BinaryOperatorExpression,
                                    merge: (Seq[Expression], Seq[Expression]) => Seq[Expression],
                                    copy: (Expression, Expression) => Expression): Expression = {
    val rewriter = inRewriter(collectInPredicates(merge)(binary.lhs, binary.rhs))
    val newLhs = binary.lhs.endoRewrite(rewriter)
    val newRhs = binary.rhs.endoRewrite(rewriter)
    if (newLhs == newRhs) newLhs
    else copy(newLhs, newRhs)
  }

  //Rewrites a IN [] by using the the provided map of precomputed lists
  //a IN ... is rewritten to a IN inPredicates(a)
  private def inRewriter(inPredicates: Map[Expression, Seq[Expression]]) = bottomUp(Rewriter.lift({
    case in@In(a, list@ListLiteral(_)) =>
      val expressions = inPredicates(a)
      if (expressions.nonEmpty) in.copy(rhs = list.copy(expressions)(list.position))(in.position)
      else False()(in.position)
  }))

  //Given `a IN A ... b IN B ... a IN C` and use `merge` to merge all the lists with the same key.
  //Returns {a -> merge(A,B), b -> C}
  private def collectInPredicates(merge: (Seq[Expression], Seq[Expression]) => Seq[Expression])
                                 (expressions: Expression*): Map[Expression, Seq[Expression]] = {
    val maps = expressions.map(_.treeFold(Map.empty[Expression, Seq[Expression]]) {
      case In(a, ListLiteral(exprs)) => (map) => {
        //if there is already a list associated with `a`, do map(a) ++ exprs otherwise exprs
        val values = map.get(a).map(current => merge(current, exprs)).getOrElse(exprs).distinct
        (map + (a -> values), None)
      }
    })
    //Take list of maps, [map1,map2,...] and merge the using the provided `merge` to
    //merge lists
    maps.reduceLeft((acc, current) => {
      val sharedKeys = acc.keySet intersect current.keySet
      val updates = sharedKeys.map(k => k -> merge(acc(k), current(k)).distinct)
      acc ++ updates ++ (current -- sharedKeys)
    })
  }
}