/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.graphdb.Direction

class extractPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {
  test("()-[*]->()") {
    val (nodePredicates: Seq[Expression], edgePredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(), "r", "r-edge", "r-node", "n")

    nodePredicates shouldBe empty
    edgePredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("(n)-[r* {prop: 42}]->()") {
    val rewrittenPredicate = AllIterablePredicate(FilterScope(varFor("  FRESHID15"), Some(propEquality("  FRESHID15", "prop", 42)))(pos), varFor("r"))(pos)

    val (nodePredicates: Seq[Expression], edgePredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-edge", "r-node", "n")

    nodePredicates shouldBe empty
    edgePredicates shouldBe List(Equals(Property(varFor("r-edge"), PropertyKeyName("prop")(pos))(pos), literalInt(42))(pos))
    solvedPredicates shouldBe List(rewrittenPredicate)
  }

  test("(n)-[x*]->() WHERE ALL(r in x WHERE r.prop < 4)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      FunctionInvocation(
        FunctionName("relationships")(pos),
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, NilPathStep)))(pos))(pos))(pos)

    val (nodePredicates: Seq[Expression], edgePredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "x", "x-edge", "x-node", "n")

    nodePredicates shouldBe empty
    edgePredicates shouldBe List(LessThan(Property(varFor("x-edge"), PropertyKeyName("prop")(pos))(pos), literalInt(4))(pos))
    solvedPredicates shouldBe List(rewrittenPredicate)
  }

  test("p = (n)-[x*]->() WHERE NONE(r in rels(p) WHERE r.prop < 4) AND ALL(m in nodes(p) WHERE exists(m.prop))") {

    val rewrittenRelPredicate = NoneIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      FunctionInvocation(
        FunctionName("relationships")(pos),
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, NilPathStep)))(pos))(pos))(pos)

    val rewrittenNodePredicate = AllIterablePredicate(
      FilterScope(varFor("m"), Some(FunctionInvocation(FunctionName("exists")(pos), Property(Variable("m")(pos),PropertyKeyName("prop")(pos))(pos))(pos)))(pos),
      FunctionInvocation(
        FunctionName("nodes")(pos),
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, NilPathStep)))(pos))(pos))(pos)

    val (nodePredicates: Seq[Expression], edgePredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenRelPredicate, rewrittenNodePredicate), "x", "x-edge", "x-node", "n")

    nodePredicates shouldBe List(FunctionInvocation(FunctionName("exists")(pos),Property(Variable("x-node")(pos),PropertyKeyName("prop")(pos))(pos))(pos))
    edgePredicates shouldBe List(Not(LessThan(Property(varFor("x-edge"), PropertyKeyName("prop")(pos))(pos), literalInt(4))(pos))(pos))
    solvedPredicates shouldBe List(rewrittenRelPredicate, rewrittenNodePredicate)
  }
}
