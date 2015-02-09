/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.PlannerQuery
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, AllNodesScan}
import org.neo4j.cypher.internal.compiler.v2_2.{DummyPosition, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.graphdb.Direction

class GetDegreeOptimizerTest extends CypherFunSuite with RewriteTest {

  val pos = DummyPosition(0)

  test("rewrites length function for a simple pattern") {
    val statement = parseForRewriting("MATCH n WHERE LENGTH((n)-->()) RETURN n")
    val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

    wherePredicate should equal(GetDegree(Identifier("n")(pos), None, Direction.OUTGOING)(pos))
  }

  test("rewrites length function for a simple pattern, incoming") {
    val statement = parseForRewriting("MATCH n WHERE LENGTH((n)<--()) RETURN n")
    val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

    wherePredicate should equal(GetDegree(Identifier("n")(pos), None, Direction.INCOMING)(pos))
  }

  test("rewrites length function for a simple pattern, both ways") {
    val statement = parseForRewriting("MATCH n WHERE LENGTH((n)--()) RETURN n")
    val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

    wherePredicate should equal(GetDegree(Identifier("n")(pos), None, Direction.BOTH)(pos))
  }

  test("rewrites length function for a simple pattern with reltype") {
    val statement = parseForRewriting("MATCH n WHERE LENGTH((n)-[:X]->()) RETURN n")
    val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

    wherePredicate should equal(GetDegree(Identifier("n")(pos), Some(RelTypeName("X")(pos)), Direction.OUTGOING)(pos))
  }

  test("rewrites length function for a simple pattern with reltype where the known is on the RHS") {
    val statement = parseForRewriting("MATCH n WHERE LENGTH(()-[:X]->(n)) RETURN n")
    val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

    wherePredicate should equal(GetDegree(Identifier("n")(pos), Some(RelTypeName("X")(pos)), Direction.INCOMING)(pos))
  }

  test("rewrites length function for a simple pattern with OR reltypes") {
    val statement = parseForRewriting("MATCH n WHERE LENGTH((n)-[:X|Y]->()) RETURN n")
    val wherePredicate = findWherePredicate(statement.endoRewrite(getDegreeOptimizer))

    wherePredicate should equal(
      Add(
        GetDegree(Identifier("n")(pos), Some(RelTypeName("X")(pos)), Direction.OUTGOING)(pos),
        GetDegree(Identifier("n")(pos), Some(RelTypeName("Y")(pos)), Direction.OUTGOING)(pos))(pos))
  }

  test("does not rewrite patterns where the unbound parts of the pattern have constraints on them") {
    doesNotRewrite("MATCH n WHERE LENGTH((n)-[ {key: 42} ]->()) RETURN n")
    doesNotRewrite("MATCH n WHERE LENGTH((n)-->( {key: 42} )) RETURN n")
    doesNotRewrite("MATCH n WHERE LENGTH((n)-->(:LABEL) ) RETURN n")
  }

  test("does not rewrite expressions inside nested plans") {
    val patternStatement = parseForRewriting("MATCH n WHERE (n)-[:X]->({prop: LENGTH((n)-[:X]->())}) RETURN n")
    val patternExpression = findWherePredicate(patternStatement).asInstanceOf[PatternExpression]
    val nestedPlanExpression = NestedPlanExpression(AllNodesScan(IdName("a"), Set.empty)(PlannerQuery.empty), patternExpression)(pos)

    val degreeStatement = parseForRewriting("MATCH n WHERE LENGTH((n)-[:X]->()) RETURN n")
    val degreeExpression = findWherePredicate(degreeStatement)

    val expression = And(degreeExpression, nestedPlanExpression)(pos)

    val result = expression.endoRewrite(getDegreeOptimizer)

    val And(left, right) = result

    left.isInstanceOf[GetDegree] should be(right = true)
    right should equal(nestedPlanExpression)
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
