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
package org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.FreshIdNameGenerator

object PropertyPredicateNormalizer extends MatchPredicateNormalizer {
  override val extract: PartialFunction[AnyRef, IndexedSeq[Expression]] = {
    case NodePattern(Some(id), _, Some(props)) if !isParameter(props) =>
      propertyPredicates(id, props)

    case RelationshipPattern(Some(id), _, None, Some(props), _, _) if !isParameter(props) =>
      propertyPredicates(id, props)

    case rp@RelationshipPattern(Some(id), _, Some(_), Some(props), _, _) if !isParameter(props) =>
      Vector(varLengthPropertyPredicates(id, props, rp.position))
  }

  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p@NodePattern(Some(_) ,_, Some(props)) if !isParameter(props)                  => p.copy(properties = None)(p.position)
    case p@RelationshipPattern(Some(_), _, _, Some(props), _, _) if !isParameter(props) => p.copy(properties = None)(p.position)
  }

  private def isParameter(expr: Expression) = expr match {
    case _: Parameter => true
    case _            => false
  }

  private def propertyPredicates(id: Variable, props: Expression): IndexedSeq[Expression] = props match {
    case mapProps: MapExpression =>
      mapProps.items.map {
        // MATCH (a {a: 1, b: 2}) => MATCH (a) WHERE a.a = 1 AND a.b = 2
        case (propId, expression) => Equals(Property(id.copyId, propId)(mapProps.position), expression)(mapProps.position)
      }.toIndexedSeq
    case expr: Expression =>
      Vector(Equals(id.copyId, expr)(expr.position))
    case _ =>
      Vector.empty
  }

  private def varLengthPropertyPredicates(id: Variable, props: Expression, patternPosition: InputPosition): Expression = {
    val idName = FreshIdNameGenerator.name(patternPosition)
    val newId = Variable(idName)(id.position)
    val expressions = propertyPredicates(newId, props)
    val conjunction = conjunct(expressions)
    AllIterablePredicate(newId, id.copyId, Some(conjunction))(props.position)
  }

  private def conjunct(exprs: Seq[Expression]): Expression = exprs match {
    case Nil           => throw new IllegalArgumentException("There should be at least one predicate to be rewritten")
    case expr +: Nil   => expr
    case expr +: tail  => And(expr, conjunct(tail))(expr.position)
  }
}
