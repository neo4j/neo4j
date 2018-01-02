/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{AllNodesScan, IdName}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, Rewriter, SemanticDirection}

class GetDegreeOptimizerTest extends CypherFunSuite with RewriteTest {

  import org.scalatest.prop.TableDrivenPropertyChecks._

  val pos = DummyPosition(0)

  val functionNames = Table("fun", "LENGTH", "SIZE")

  test("rewrites length function for a simple pattern") {
    forAll(functionNames) { fun =>
      val statement = parseForRewriting(s"MATCH n WHERE $fun((n)-->()) RETURN n")
      val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

      wherePredicate should equal(GetDegree(Identifier("n")(pos), None, SemanticDirection.OUTGOING)(pos))
    }
  }


  test("rewrites length function for a simple pattern, incoming") {
    forAll(functionNames) { fun =>
      val statement = parseForRewriting(s"MATCH n WHERE $fun((n)<--()) RETURN n")
      val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

      wherePredicate should equal(GetDegree(Identifier("n")(pos), None, SemanticDirection.INCOMING)(pos))
    }
  }

  test("rewrites length function for a simple pattern, both ways") {
    forAll(functionNames) { fun =>
      val statement = parseForRewriting(s"MATCH n WHERE $fun((n)--()) RETURN n")
      val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

      wherePredicate should equal(GetDegree(Identifier("n")(pos), None, SemanticDirection.BOTH)(pos))
    }
  }

  test("rewrites length function for a simple pattern with reltype") {
    forAll(functionNames) { fun =>
      val statement = parseForRewriting(s"MATCH n WHERE $fun((n)-[:X]->()) RETURN n")
      val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

      wherePredicate should equal(GetDegree(Identifier("n")(pos), Some(RelTypeName("X")(pos)), SemanticDirection.OUTGOING)(pos))
    }
  }

  test("rewrites length function for a simple pattern with reltype where the known is on the RHS") {
    forAll(functionNames) { fun =>
      val statement = parseForRewriting(s"MATCH n WHERE $fun(()-[:X]->(n)) RETURN n")
      val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

      wherePredicate should equal(GetDegree(Identifier("n")(pos), Some(RelTypeName("X")(pos)), SemanticDirection.INCOMING)(pos))
    }
  }

  test("rewrites length function for a simple pattern with OR reltypes") {
    forAll(functionNames) { fun =>
      val statement = parseForRewriting(s"MATCH n WHERE $fun((n)-[:X|Y]->()) RETURN n")
      val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

      wherePredicate should equal(
        Add(
          GetDegree(Identifier("n")(pos), Some(RelTypeName("X")(pos)), SemanticDirection.OUTGOING)(pos),
          GetDegree(Identifier("n")(pos), Some(RelTypeName("Y")(pos)), SemanticDirection.OUTGOING)(pos))(pos))
    }
  }

  test("does not rewrite patterns where the unbound parts of the pattern have constraints on them") {
    forAll(functionNames) { fun =>
      doesNotRewrite(s"MATCH n WHERE $fun((n)-[ {key: 42} ]->()) RETURN n")
      doesNotRewrite(s"MATCH n WHERE $fun((n)-->( {key: 42} )) RETURN n")
      doesNotRewrite(s"MATCH n WHERE $fun((n)-->(:LABEL) ) RETURN n")
    }
  }

  test("does not rewrite expressions inside nested plans") {
    forAll(functionNames) { fun =>
      val patternStatement = parseForRewriting(s"MATCH n WHERE (n)-[:X]->({prop: $fun((n)-[:X]->())}) RETURN n")
      val patternExpression = findWherePredicate(patternStatement).asInstanceOf[PatternExpression]
      val nestedPlanExpression = NestedPlanExpression(AllNodesScan(IdName("a"), Set.empty)(null), patternExpression)(pos)

      val degreeStatement = parseForRewriting(s"MATCH n WHERE $fun((n)-[:X]->()) RETURN n")
      val degreeExpression = findWherePredicate(degreeStatement)

      val expression = And(degreeExpression, nestedPlanExpression)(pos)

      val result = expression.endoRewrite(getDegreeOptimizer)

      val And(left, right) = result

      left.isInstanceOf[GetDegree] should be(right = true)
      right should equal(nestedPlanExpression)
    }
  }

  private def doesNotRewrite(q: String) {
    val statement = parseForRewriting(q)
    val rewrittenStatement = statement.endoRewrite(getDegreeOptimizer)
    val rewritten = findWherePredicate(rewrittenStatement)
    val original = findWherePredicate(statement)

    rewritten should equal(original)
  }

  def findWherePredicate(x: Statement): Expression =
    x.findByClass[Where].expression

  def rewriterUnderTest: Rewriter = getDegreeOptimizer
}
