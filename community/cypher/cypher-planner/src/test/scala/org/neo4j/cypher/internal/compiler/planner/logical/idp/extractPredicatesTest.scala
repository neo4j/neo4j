/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class extractPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  test("()-[*]->()") {
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(Seq(), v"r", v"n", v"  UNNAMED2", targetNodeIsBound = false, VarPatternLength.unlimited)

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("(n)-[r* {prop: 42}]->()") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("  FRESHID15"), Some(propEquality("  FRESHID15", "prop", 42)))(pos),
      varFor("r")
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"n",
        v"  UNNAMED0",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(VariablePredicate(
      varFor("  FRESHID15"),
      propEquality("  FRESHID15", "prop", 42)
    ))
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("(n)-[r* {prop: 42}]->() with bound variable") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("  FRESHID15"), Some(propEquality("  FRESHID15", "prop", 42)))(pos),
      varFor("r")
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"n",
        v"  FRESHID15",
        targetNodeIsBound = true,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(VariablePredicate(
      varFor("  FRESHID15"),
      propEquality("  FRESHID15", "prop", 42)
    ))
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("(n)-[x*]->(m) WHERE ALL(r in x WHERE  r.prop < 4)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      function(
        "relationships",
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(
              pos
            )
          )(pos)
        )(pos)
      )
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"x",
        v"n",
        v"m",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(VariablePredicate(varFor("r"), propLessThan("r", "prop", 4)))
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test(
    "p = (n)-[x*]->(o) WHERE NONE(r in relationships(p) WHERE r.prop < 4) AND ALL(m in nodes(p) WHERE m.prop IS NOT NULL)"
  ) {

    val rewrittenRelPredicate = NoneIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      function(
        "relationships",
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, Some(varFor("0")), NilPathStep()(pos))(
              pos
            )
          )(pos)
        )(pos)
      )
    )(pos)

    val rewrittenNodePredicate = AllIterablePredicate(
      FilterScope(varFor("m"), Some(isNotNull(prop("m", "prop"))))(pos),
      function(
        "nodes",
        PathExpression(
          NodePathStep(
            varFor("n"),
            MultiRelationshipPathStep(varFor("x"), SemanticDirection.OUTGOING, Some(varFor("o")), NilPathStep()(pos))(
              pos
            )
          )(pos)
        )(pos)
      )
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenRelPredicate, rewrittenNodePredicate),
        v"x",
        v"n",
        v"o",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe ListSet(VariablePredicate(varFor("m"), isNotNull(prop("m", "prop"))))
    relationshipPredicates shouldBe ListSet(VariablePredicate(varFor("r"), not(propLessThan("r", "prop", 4))))
    solvedPredicates shouldBe ListSet(rewrittenRelPredicate, rewrittenNodePredicate)
  }

  test("p = (n)-[r*1]->(m) WHERE ALL (x IN nodes(p) WHERE x.prop = n.prop") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val nodePredicate = equals(prop("x", "prop"), prop("n", "prop"))
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(nodePredicate))(pos),
      function("nodes", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"n",
        v"m",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe ListSet(VariablePredicate(varFor("x"), equals(prop("x", "prop"), prop("n", "prop"))))
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("p = (n)-[r*1]->(m) WHERE ALL (x IN nodes(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("nodes", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"n",
        v"m",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p=(n)-[rel:*1..3]->(m) WHERE ALL (x in nodes(p) WHERE x = n or x = m)") {

    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("rel"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(ors(equals(varFor("x"), varFor("n")), equals(varFor("x"), varFor("m")))))(pos),
      function("nodes", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"rel",
        v"n",
        v"m",
        targetNodeIsBound = false,
        VarPatternLength(1, Some(3))
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = (n)-[r*1]->() WHERE ALL (x IN relationships(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("relationships", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"n",
        v"  UNNAMED0",
        targetNodeIsBound = false,
        VarPatternLength.fixed(1)
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = (n)-[r*1]->() WHERE NONE (x IN nodes(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val rewrittenPredicate = NoneIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("nodes", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"n",
        v"  UNNAMED0",
        targetNodeIsBound = false,
        VarPatternLength.fixed(1)
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = (n)-[r*1]->(m) WHERE NONE (x IN relationships(p) WHERE length(p) = 1)") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val rewrittenPredicate = NoneIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", pathExpression), literalInt(1))))(pos),
      function("relationships", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(Seq(rewrittenPredicate), v"r", v"n", v"m", targetNodeIsBound = false, VarPatternLength.fixed(1))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  // "MATCH (a:A)-[r* {aProp: a.prop, bProp: b.prop}]->(b)"
  test("p = (a)-[r*]->(b) WHERE ALL (x IN relationships(p) WHERE x.aProp = a.prop AND x.bProp = b.prop)") {
    val pathExpression = PathExpressionBuilder.node("a").outToVarLength("r", "b").build()

    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          ands(
            equals(prop("x", "aProp"), prop("a", "prop")),
            equals(prop("x", "bProp"), prop("b", "prop"))
          )
        )
      )(pos),
      function("relationships", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(rewrittenPredicate),
        v"r",
        v"a",
        v"b",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  // "MATCH (a:A)-[r* {aProp: a.prop, bProp: b.prop}]->(b)"
  test(
    "p = (a)-[r*]->(b) WHERE ALL (x IN relationships(p) WHERE x.aProp = a.prop) AND ALL (x IN relationships(p) WHERE x.bProp = b.prop)"
  ) {
    val pathExpression = PathExpressionBuilder.node("a").outToVarLength("r", "b").build()

    val solvableAllPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          equals(prop("x", "aProp"), prop("a", "prop"))
        )
      )(pos),
      function("relationships", pathExpression)
    )(pos)
    val dependingAllPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          equals(prop("x", "bProp"), prop("b", "prop"))
        )
      )(pos),
      function("relationships", pathExpression)
    )(pos)
    val rewrittenPredicates = Seq(
      solvableAllPredicate,
      dependingAllPredicate
    )

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(rewrittenPredicates, v"r", v"a", v"b", targetNodeIsBound = false, VarPatternLength.unlimited)

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(
      VariablePredicate(varFor("x"), equals(prop("x", "aProp"), prop("a", "prop")))
    )
    solvedPredicates shouldBe ListSet(solvableAllPredicate)
  }

  test("p = (n)-[r*1]->(m) WHERE ALL (x IN nodes(p) WHERE x.prop < m.prop) with bound variable") {
    val pathExpression = PathExpression(
      NodePathStep(
        varFor("n"),
        MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("m")), NilPathStep()(pos))(pos)
      )(pos)
    )(pos)

    val nodePredicate = lessThan(prop("x", "prop"), prop("m", "prop"))
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(nodePredicate))(pos),
      function("nodes", pathExpression)
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(Seq(rewrittenPredicate), v"r", v"n", v"m", targetNodeIsBound = true, VarPatternLength.fixed(1))

    nodePredicates shouldBe ListSet(VariablePredicate(varFor("x"), lessThan(prop("x", "prop"), prop("m", "prop"))))
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("should mark uniqueness predicate as solved") {
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(unique(varFor("r")), unique(varFor("other_rel"))),
        v"r",
        v"n",
        v"m",
        targetNodeIsBound = false,
        VarPatternLength.unlimited
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe ListSet(unique(varFor("r")))
  }

  test("should extract VarLengthBound predicates on exact match") {
    val varLengthPredicates = Seq(varLengthLowerLimitPredicate("rel", 42), varLengthUpperLimitPredicate("rel", 42))
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        varLengthPredicates,
        v"rel",
        v"n",
        v"m",
        targetNodeIsBound = false,
        varLength = VarPatternLength(42, Some(42))
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe varLengthPredicates.toSet
  }

  test("should not extract VarLengthBound predicates if on other relationship") {
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(varLengthLowerLimitPredicate("rel", 42), varLengthUpperLimitPredicate("rel", 42)),
        v"rel2",
        v"n",
        v"m",
        targetNodeIsBound = false,
        varLength = VarPatternLength(42, Some(42))
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("should not extract more specific VarLengthBound predicates") {
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(varLengthLowerLimitPredicate("rel", 42), varLengthUpperLimitPredicate("rel", 42)),
        v"rel",
        v"n",
        v"m",
        targetNodeIsBound = false,
        varLength = VarPatternLength(40, Some(45))
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("should extract only VarLengthBound predicates which would be satisfied by the var-length relationship given") {
    val lowerBoundPredicate = varLengthLowerLimitPredicate("rel", 40)
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractPredicates(
        Seq(lowerBoundPredicate, varLengthUpperLimitPredicate("rel", 42)),
        v"rel",
        v"n",
        v"m",
        targetNodeIsBound = false,
        varLength = VarPatternLength(42, Some(45))
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe ListSet(lowerBoundPredicate)
  }

  test("should extract predicates regardless of function name spelling") {

    def makePredicate(funcName: String, negatedPredicate: Boolean) = {
      val pred = lessThan(prop("m", "prop"), literalInt(123))

      // p = (a)-[r*]->(b)
      val pathExpr = PathExpression(
        NodePathStep(
          varFor("a"),
          MultiRelationshipPathStep(varFor("r"), SemanticDirection.OUTGOING, Some(varFor("b")), NilPathStep()(pos))(
            pos
          )
        )(pos)
      )(pos)

      val iterablePredicate = if (negatedPredicate)
        NoneIterablePredicate(varFor("m"), function(funcName, pathExpr), Some(pred))(pos)
      else
        AllIterablePredicate(varFor("m"), function(funcName, pathExpr), Some(pred))(pos)

      val solvedPredicate = VariablePredicate(varFor("m"), if (negatedPredicate) not(pred) else pred)
      (iterablePredicate, solvedPredicate)
    }

    val functionNames = Seq(("nodes", "relationships"), ("NODES", "RELATIONSHIPS"))
    for ((nodesF, relationshipsF) <- functionNames) withClue((nodesF, relationshipsF)) {
      val (allNode, allSolvedNode) = makePredicate(nodesF, negatedPredicate = false)
      val (allRel, allSolvedRel) = makePredicate(relationshipsF, negatedPredicate = false)
      val (noneNode, noneSolvedNode) = makePredicate(nodesF, negatedPredicate = true)
      val (noneRel, noneSolvedRel) = makePredicate(relationshipsF, negatedPredicate = true)

      val (nodePredicates, relationshipPredicates, solvedPredicates) =
        extractPredicates(
          Seq(allNode, allRel, noneNode, noneRel),
          v"r",
          v"a",
          v"b",
          targetNodeIsBound = false,
          VarPatternLength.unlimited
        )

      nodePredicates shouldBe ListSet(allSolvedNode, noneSolvedNode)
      relationshipPredicates shouldBe ListSet(allSolvedRel, noneSolvedRel)
      solvedPredicates shouldBe ListSet(allNode, allRel, noneNode, noneRel)
    }
  }
}
