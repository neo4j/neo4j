/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator

case class PropertyPredicateNormalizer(anonymousVariableNameGenerator: AnonymousVariableNameGenerator)
    extends PredicateNormalizer {

  override val extract: PartialFunction[AnyRef, IndexedSeq[Expression]] = {
    case NodePattern(Some(id), _, Some(props: MapExpression), _) =>
      propertyPredicates(id, props)

    case RelationshipPattern(Some(id), _, None, Some(props: MapExpression), _, _) =>
      propertyPredicates(id, props)

    case RelationshipPattern(Some(id), _, Some(_), Some(props: MapExpression), _, _) if props.items.nonEmpty =>
      Vector(varLengthPropertyPredicates(id, props))
  }

  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p @ NodePattern(Some(_), _, Some(_: MapExpression), _) =>
      p.copy(properties = None)(p.position)
    case NodePattern(Some(id), _, Some(_), _) =>
      throw new IllegalStateException(s"Node pattern $id with non-map properties.")
    case p @ RelationshipPattern(Some(_), _, _, Some(_: MapExpression), _, _) =>
      p.copy(properties = None)(p.position)
    case RelationshipPattern(Some(id), _, _, Some(_), _, _) =>
      throw new IllegalStateException(s"Relationship pattern $id with non-map properties.")
  }

  private def propertyPredicates(id: LogicalVariable, mapProps: MapExpression): IndexedSeq[Equals] =
    mapProps.items.map {
      // MATCH (a {a: 1, b: 2}) => MATCH (a) WHERE a.a = 1 AND a.b = 2
      case (propId, expression) =>
        Equals(Property(id.copyId, propId)(mapProps.position), expression)(mapProps.position)
    }.toIndexedSeq

  private def varLengthPropertyPredicates(id: LogicalVariable, props: MapExpression): Expression = {
    val idName = anonymousVariableNameGenerator.nextName
    val newId = Variable(idName)(id.position)
    val expressions = propertyPredicates(newId, props)
    val conjunction = conjunct(expressions.toList)
    AllIterablePredicate(newId, id.copyId, Some(conjunction))(props.position)
  }

  private def conjunct(exprs: List[Expression]): Expression = exprs match {
    case Nil          => throw new IllegalArgumentException("There should be at least one predicate to be rewritten")
    case expr :: Nil  => expr
    case expr :: tail => And(expr, conjunct(tail))(expr.position)
  }
}
