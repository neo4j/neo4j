/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.expressions._

class extractPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {
  test("()-[*]->()") {
    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("(n)-[r* {prop: 42}]->()") {
    val rewrittenPredicate = AllIterablePredicate(FilterScope(varFor("  FRESHID15"), Some(propEquality("  FRESHID15", "prop", 42)))(pos), varFor("r"))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe List(propEquality("r-relationship", "prop", 42))
    solvedPredicates shouldBe List(rewrittenPredicate)
  }

  test("(n)-[x*]->(m) WHERE ALL(r in x WHERE r.prop < 4)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      function(
        "relationships",
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep)))(pos)))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "x", "x-relationship", "x-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe List(propLessThan("x-relationship", "prop", 4))
    solvedPredicates shouldBe List(rewrittenPredicate)
  }

  test("p = (n)-[x*]->(o) WHERE NONE(r in relationships(p) WHERE r.prop < 4) AND ALL(m in nodes(p) WHERE exists(m.prop))") {

    val rewrittenRelPredicate = NoneIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      function(
        "relationships",
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, Some(varFor("0")), NilPathStep)))(pos)))(pos)

    val rewrittenNodePredicate = AllIterablePredicate(
      FilterScope(varFor("m"), Some(function("exists", prop("m", "prop"))))(pos),
      function(
        "nodes",
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, Some(varFor("o")), NilPathStep)))(pos)))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenRelPredicate, rewrittenNodePredicate), "x", "x-relationship", "x-node", "n")

    nodePredicates shouldBe List(function("exists", prop("x-node", "prop")))
    relationshipPredicates shouldBe List(not(propLessThan("x-relationship", "prop", 4)))
    solvedPredicates shouldBe List(rewrittenRelPredicate, rewrittenNodePredicate)
  }

  test("p = (n)-[r*1]->(m) WHERE ALL (x IN nodes(p) WHERE x.prop = n.prop") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"),SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep)))(pos)

    val nodePredicate = equals(prop("x", "prop"), prop("n", "prop"))
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(nodePredicate))(pos),
      function("nodes", pathExpression))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe List(equals(prop("r-node", "prop"), prop("n", "prop")))
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe List(rewrittenPredicate)
  }

  test("p = (n)-[r*1]->(m) WHERE ALL (x IN nodes(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"),SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep)))(pos)

    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("nodes", pathExpression))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = (n)-[r*1]->() WHERE ALL (x IN relationships(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"),SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep)))(pos)

    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("relationships", pathExpression))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = (n)-[r*1]->() WHERE NONE (x IN nodes(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep)))(pos)

    val rewrittenPredicate = NoneIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("nodes", pathExpression))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = (n)-[r*1]->(m) WHERE NONE (x IN relationships(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"),SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep)))(pos)

    val rewrittenPredicate = NoneIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("relationships", pathExpression))(pos)

    val (nodePredicates: Seq[Expression], relationshipPredicates: Seq[Expression], solvedPredicates: Seq[Expression]) =
      extractPredicates(Seq(rewrittenPredicate), "r", "r-relationship", "r-node", "n")

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }
}
