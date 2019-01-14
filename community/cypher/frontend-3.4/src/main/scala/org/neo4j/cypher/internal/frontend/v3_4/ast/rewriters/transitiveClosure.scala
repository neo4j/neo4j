/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseContext, Condition}
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions._

/**
  * Transitive closure of where clauses.
  *
  * Given a where clause, `WHERE a.prop = b.prop AND b.prop = 42` we rewrite the query
  * into `WHERE a.prop = 42 AND b.prop = 42`
  */
case object transitiveClosure extends StatementRewriter {

  override def description: String = "transitive closure in where clauses"

  override def instance(ignored: BaseContext): Rewriter = transitiveClosureRewriter

  override def postConditions: Set[Condition] = Set.empty

  private case object transitiveClosureRewriter extends Rewriter {

    def apply(that: AnyRef): AnyRef = instance.apply(that)

    private val instance: Rewriter = bottomUp(Rewriter.lift {
      case where: Where => fixedPoint((w: Where) => w.endoRewrite(whereRewriter))(where)
    })

    //Collects property equalities, e.g `a.prop = 42`
    private def collect(e: Expression): Closures = e.treeFold(Closures.empty) {
      case _: Or => (acc) => (acc, None)
      case _: And => (acc) => (acc, Some(identity))
      case Equals(p1: Property, p2: Property) => (acc) => (acc.withEquivalence(p1 -> p2), None)
      case Equals(p: Property, other) => (acc) => (acc.withMapping(p -> other), None)
      case Not(Equals(_,_)) => (acc) => (acc, None)
    }

    //NOTE that this might introduce duplicate predicates, however at a later rewrite
    //when AND is turned into ANDS we remove all duplicates
    private val whereRewriter: Rewriter = bottomUp(Rewriter.lift {
      case and@And(lhs, rhs) =>
        val closures = collect(lhs) ++ collect(rhs)
        val inner = andRewriter(closures)
        val newAnd = and.copy(lhs = lhs.endoRewrite(inner), rhs = rhs.endoRewrite(inner))(and.position)

        //ALSO take care of case WHERE b.prop = a.prop AND b.prop = 42
        //turns into WHERE b.prop = a.prop AND b.prop = 42 AND a.prop = 42
        closures.emergentEqualities.foldLeft(newAnd) {
          case (acc, (prop, expr)) => And(acc, Equals(prop, expr)(acc.position))(acc.position)
        }
    })

    private def andRewriter(closures: Closures): Rewriter = {
      val stopOnNotEquals: AnyRef => Boolean = {
        case not@Not(Equals(_, _)) => true
        case _ => false
      }

      bottomUp(Rewriter.lift {
        case equals@Equals(p1: Property, p2: Property) if closures.mapping.contains(p2) =>
          equals.copy(rhs = closures.mapping(p2))(equals.position)
      }, stopOnNotEquals)
    }
  }

  case class Closures(mapping: Map[Property, Expression] = Map.empty,
                      equivalence: Map[Property, Property] = Map.empty) {

    def withMapping(e:(Property,Expression)): Closures = copy(mapping = mapping + e)
    def withEquivalence(e:(Property,Property)): Closures = copy(equivalence = equivalence + e )

    def emergentEqualities: Map[Property, Expression] = {
      val sharedKeys = equivalence.keySet.intersect(mapping.keySet)

      sharedKeys.map(k => equivalence(k) -> mapping(k)).toMap -- mapping.keySet
    }

    def ++(other: Closures): Closures = copy(mapping ++ other.mapping, equivalence ++ other.equivalence)
  }

  object Closures {
    def empty = Closures()
  }

}
