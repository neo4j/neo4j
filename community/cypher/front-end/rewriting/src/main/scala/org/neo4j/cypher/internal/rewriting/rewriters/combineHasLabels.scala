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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

/**
 * Optimisation that combines multiple has label predicates, like
 * `n:SomeLabel OR n:OtherLabel` into the specialised `HasAnyLabel` expression.
 */
case object combineHasLabels extends Rewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val instance: Rewriter = bottomUp(
    Rewriter.lift { case ors: Ors => rewrite(ors) }
  )

  private def rewrite(ors: Ors): Expression = {
    val (lonelyHasLabels, nonRewritable) = ors.exprs.partitionMap {
      case hasLabels @ HasLabels(_, labels) if labels.size == 1 => Left(hasLabels)
      case expression                                           => Right(expression)
    }

    val rewrittenHasLabels = ListSet.from(
      lonelyHasLabels
        .groupBy(_.expression)
        .map {
          case (_, singleHasLabels) if singleHasLabels.size == 1 => singleHasLabels.head
          case (entity, hasLabels) => HasAnyLabel(entity, hasLabels.flatMap(_.labels).toSeq)(entity.position)
        }
    )

    if (rewrittenHasLabels.size == lonelyHasLabels.size) {
      ors
    } else {
      val predicates = nonRewritable ++ rewrittenHasLabels
      predicates match {
        case singlePredicate if singlePredicate.size == 1 => singlePredicate.head
        case manyPredicates                               => Ors(manyPredicates)(ors.position)
      }
    }
  }
}
