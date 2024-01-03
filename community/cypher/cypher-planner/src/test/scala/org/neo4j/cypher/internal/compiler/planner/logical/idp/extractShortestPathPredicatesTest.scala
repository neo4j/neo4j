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
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class extractShortestPathPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  test("p=shortestPath((a)-[*]->(b))") {
    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(), Some(v"p"), None)

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("MATCH (n), (m), p=shortestPath((n)-[rs* ]->(m)) WHERE all(r IN rs WHERE r.prop=42)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("  FRESHID15"), Some(propEquality("  FRESHID15", "prop", 42)))(pos),
      varFor("rs")
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), None, Some(v"rs"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(VariablePredicate(
      varFor("  FRESHID15"),
      propEquality("  FRESHID15", "prop", 42)
    ))
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("MATCH (n), (m), shortestPath((n)-[x*]->(m)) WHERE ALL(r in x WHERE r.prop < 4)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      varFor("x")
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), None, Some(v"x"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(VariablePredicate(varFor("r"), propLessThan("r", "prop", 4)))
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test(
    "p = shortestPath((n)-[x*]->(o)) WHERE NONE(r in relationships(p) WHERE r.prop < 4) AND ALL(m in nodes(p) WHERE m.prop IS NOT NULL)"
  ) {
    val rewrittenRelPredicate = NoneIterablePredicate(
      FilterScope(varFor("r"), Some(propLessThan("r", "prop", 4)))(pos),
      function(
        "relationships",
        varFor("p")
      )
    )(pos)

    val rewrittenNodePredicate = AllIterablePredicate(
      FilterScope(varFor("m"), Some(isNotNull(prop("m", "prop"))))(pos),
      function(
        "nodes",
        varFor("p")
      )
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenRelPredicate, rewrittenNodePredicate), Some(v"p"), Some(v"x"))

    nodePredicates shouldBe ListSet(VariablePredicate(varFor("m"), isNotNull(prop("m", "prop"))))
    relationshipPredicates shouldBe ListSet(VariablePredicate(varFor("r"), not(propLessThan("r", "prop", 4))))
    solvedPredicates shouldBe ListSet(rewrittenRelPredicate, rewrittenNodePredicate)
  }

  test("p = shortestPath((n)-[r*]->(m)) WHERE ALL (x IN nodes(p) WHERE x.prop = n.prop") {
    val nodePredicate = equals(prop("x", "prop"), prop("n", "prop"))
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(nodePredicate))(pos),
      function("nodes", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"r"))

    nodePredicates shouldBe ListSet(VariablePredicate(varFor("x"), equals(prop("x", "prop"), prop("n", "prop"))))
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("p = shortestPath((n)-[r*]->(m)) WHERE ALL (x IN nodes(p) WHERE length(p) = 1)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", varFor("p")), literalInt(1))))(pos),
      function("nodes", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"r"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p=shortestPath((n)-[rel*]->(m)) WHERE ALL (x in nodes(p) WHERE x = n or x = m)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(ors(equals(varFor("x"), varFor("n")), equals(varFor("x"), varFor("m")))))(pos),
      function("nodes", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"rel"))

    nodePredicates shouldBe ListSet(
      VariablePredicate(varFor("x"), ors(equals(varFor("x"), varFor("n")), equals(varFor("x"), varFor("m"))))
    )
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test("p = shortestPath((n)-[r*]->(m)) WHERE ALL (x IN relationships(p) WHERE length(p) = 1)") {
    val rewrittenPredicate = AllIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", varFor("p")), literalInt(1))))(pos),
      function("relationships", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"r"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = shortestPath((n)-[r*]->(m)) WHERE NONE (x IN nodes(p) WHERE length(p) = 1)") {
    val rewrittenPredicate = NoneIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", varFor("p")), literalInt(1))))(pos),
      function("nodes", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"r"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test("p = shortestPath((n)-[r*]->(m)) WHERE NONE (x IN relationships(p) WHERE length(p) = 1)") {
    val rewrittenPredicate = NoneIterablePredicate(
      FilterScope(varFor("x"), Some(equals(function("length", varFor("p")), literalInt(1))))(pos),
      function("relationships", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"r"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe empty
    solvedPredicates shouldBe empty
  }

  test(
    "p = shortestPath((a)-[r*]->(b)) WHERE ALL (x IN relationships(p) WHERE x.aProp = a.prop AND x.bProp = b.prop)"
  ) {
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
      function("relationships", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(Set(rewrittenPredicate), Some(v"p"), Some(v"r"))

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(
      VariablePredicate(
        varFor("x"),
        ands(
          equals(prop("x", "aProp"), prop("a", "prop")),
          equals(prop("x", "bProp"), prop("b", "prop"))
        )
      )
    )
    solvedPredicates shouldBe ListSet(rewrittenPredicate)
  }

  test(
    "p = shortestPath((a)-[r*]->(b)) WHERE ALL (x IN relationships(p) WHERE x.aProp = a.prop) AND ALL (x IN relationships(p) WHERE x.bProp = b.prop)"
  ) {
    val solvableAllPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          equals(prop("x", "aProp"), prop("a", "prop"))
        )
      )(pos),
      varFor("r")
    )(pos)
    val dependingAllPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          equals(prop("x", "bProp"), prop("b", "prop"))
        )
      )(pos),
      function("relationships", varFor("p"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(
        Set(
          solvableAllPredicate,
          dependingAllPredicate
        ),
        Some(v"p"),
        Some(v"r")
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(
      VariablePredicate(varFor("x"), equals(prop("x", "aProp"), prop("a", "prop"))),
      VariablePredicate(varFor("x"), equals(prop("x", "bProp"), prop("b", "prop")))
    )
    solvedPredicates shouldBe ListSet(solvableAllPredicate, dependingAllPredicate)
  }

  test(
    "p1 = ..., p2 = shortestPath((a)-[r*]->(b)) WHERE ALL (x IN relationships(p1) WHERE x.aProp = a.prop) AND ALL (x IN relationships(p2) WHERE x.bProp = b.prop)"
  ) {
    val otherPathPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          equals(prop("x", "aProp"), prop("a", "prop"))
        )
      )(pos),
      function("relationships", varFor("p1"))
    )(pos)
    val solvableAllPredicate = AllIterablePredicate(
      FilterScope(
        varFor("x"),
        Some(
          equals(prop("x", "bProp"), prop("b", "prop"))
        )
      )(pos),
      function("relationships", varFor("p2"))
    )(pos)

    val (nodePredicates, relationshipPredicates, solvedPredicates) =
      extractShortestPathPredicates(
        Set(
          solvableAllPredicate,
          otherPathPredicate
        ),
        Some(v"p2"),
        Some(v"r")
      )

    nodePredicates shouldBe empty
    relationshipPredicates shouldBe ListSet(
      VariablePredicate(varFor("x"), equals(prop("x", "bProp"), prop("b", "prop")))
    )
    solvedPredicates shouldBe ListSet(solvableAllPredicate)
  }

  test("should extract predicates regardless of function name spelling") {

    def makePredicate(funcName: String, negatedPredicate: Boolean) = {
      val pred = lessThan(prop("m", "prop"), literalInt(123))

      val iterablePredicate = if (negatedPredicate)
        NoneIterablePredicate(varFor("m"), function(funcName, varFor("path")), Some(pred))(pos)
      else
        AllIterablePredicate(varFor("m"), function(funcName, varFor("path")), Some(pred))(pos)

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
        extractShortestPathPredicates(
          Set(allNode, allRel, noneNode, noneRel),
          path = Some(v"path"),
          rels = Some(v"r")
        )

      nodePredicates shouldBe ListSet(allSolvedNode, noneSolvedNode)
      relationshipPredicates shouldBe ListSet(allSolvedRel, noneSolvedRel)
      solvedPredicates shouldBe ListSet(allNode, allRel, noneNode, noneRel)
    }
  }

}
