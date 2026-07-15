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
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingContext
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref

class ScopeCachingTest extends VariableCheckingTestSuite with AstConstructionTestSupport {

  /* beforeRewrite -fictional rewrite-> query
   *  (test name)
   *  e.g.  a + b                       a * b
   *          v                           v
   *        scope    -no influence->    scope
   */
  {
    withClue(
      """WITH 1 AS a
        |WITH 1 AS b
        |RETURN a + b AS x""".stripMargin
    ) {
      val sharedLeadingWith =
        with_(aliasedReturnItem(literalInt(1), "a"), aliasedReturnItem(literalInt(1), "b"))
      def stmtWithClauses(cs: Clause*): Statement = singleQuery(
        (sharedLeadingWith +: cs): _*
      )

      def stmtWithReturnItems(items: ReturnItem*): Statement = stmtWithClauses(
        return_(items: _*)
      )

      val beforeRewrite = stmtWithReturnItems(aliasedReturnItem(add(varFor("a"), varFor("b")), "x"))

      test("RETURN a + b AS y") {
        val sharedExpr = add(varFor("a"), varFor("b"))
        doesNotInfluence(
          stmtWithReturnItems(aliasedReturnItem(sharedExpr, "x")),
          stmtWithReturnItems(aliasedReturnItem(sharedExpr, "y"))
        )
      }
      test("RETURN b + b AS x") {
        doesNotInfluence(
          beforeRewrite,
          stmtWithReturnItems(aliasedReturnItem(add(varFor("b"), varFor("b")), "x"))
        )
      }
      test("RETURN b + a AS x") {
        doesNotInfluence(
          beforeRewrite,
          stmtWithReturnItems(aliasedReturnItem(add(varFor("b"), varFor("a")), "x"))
        )
      }
      test("RETURN a * b AS x") {
        doesNotInfluence(
          beforeRewrite,
          stmtWithReturnItems(aliasedReturnItem(multiply(varFor("a"), varFor("b")), "x"))
        )
      }

      test("RETURN a + b + 0 AS x") {
        doesNotInfluence(
          beforeRewrite,
          stmtWithReturnItems(aliasedReturnItem(multiply(varFor("a"), varFor("b")), "x"))
        )
      }

      test("RETURN 0 + a + b AS x") {
        doesNotInfluence(
          beforeRewrite,
          stmtWithReturnItems(aliasedReturnItem(multiply(varFor("a"), varFor("b")), "x"))
        )
      }

      test(
        """WITH *
          |RETURN 0 + a + b AS x""".stripMargin
      ) {
        doesNotInfluence(
          beforeRewrite,
          stmtWithClauses(
            withAll(),
            return_(aliasedReturnItem(multiply(varFor("a"), varFor("b")), "x"))
          )
        )
      }

      test(
        """MATCH (n)
          |RETURN 0 + a + b AS x""".stripMargin
      ) {
        doesNotInfluence(
          beforeRewrite,
          stmtWithClauses(
            match_(nodePat(Some("n"))),
            return_(aliasedReturnItem(multiply(varFor("a"), varFor("b")), "x"))
          )
        )
      }

      test(
        """FINISH""".stripMargin
      ) {
        doesNotInfluence(
          beforeRewrite,
          stmtWithClauses(
            finish()
          )
        )
      }
    }
  }

  /*
   * cached subtrees get actually picked up (or not)
   */
  case class TaggedDummyASTNode(tag: String) extends ASTNode {
    override def position: InputPosition = InputPosition.NONE
    override def canEqual(that: Any): Boolean = false
    override def productArity: Int = 0
    override def productElement(n: Int): Any = ()
  }

  private def modify(modifications: (
    ASTNode,
    WorkingContext,
    String
  )*): Map[(Ref[ASTNode], WorkingContext), WorkingScopeModification] = {
    modifications.map {
      case (astNode, incoming, marker) =>
        (Ref(astNode), incoming) -> markWorkingScopeModified(
          TaggedDummyASTNode(s"${this.getClass.getSimpleName} modified '''${prettify(astNode)}''' marked $marker")
        )
    }.toMap
  }

  private def mod(
    astNode: ASTNode,
    incoming: WorkingContext,
    marker: String = ""
  ): (ASTNode, WorkingContext, String) = {
    (astNode, incoming, marker)
  }

  withClue("cache gets picked up") {
    withClue(
      """=====
        |UNWIND [1, 2, 3] AS x
        |MATCH (n {p: x})-[r:R]->()
        |RETURN x AS x
        |=====
        |""".stripMargin
    ) {
      val statement = singleQuery(
        unwind(listOfInt(1, 2, 3), varFor("x")),
        match_(
          relationshipChain(
            nodePat(Some("n"), properties = Some(mapOf("p" -> varFor("x")))),
            relPat(Some("r"), labelExpression = Some(labelRelTypeLeaf("R"))),
            nodePat()
          )
        ),
        return_(aliasedReturnItem(varFor("x")))
      )
      test("modify pattern") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              relPat(Some("r"), labelExpression = Some(labelRelTypeLeaf("R"))),
              PatternIncomingContext(
                topologicalConstants = Set(varFor("n")),
                predicateConstants = Set(varFor("n"), varFor("r")),
                pathConstants = Set.empty,
                groupConstants = Set.empty,
                localCallables = Set.empty
              )
            )
          )
        )
      }
      test("modify return item") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              aliasedReturnItem(varFor("x")),
              RegularContext(constants = Set.empty, variables = Set(varFor("x")), localCallables = Set.empty)
            )
          )
        )
      }
      test("modify expression and return item") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 2, 3),
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            ),
            mod(
              aliasedReturnItem(varFor("x")),
              RegularContext(constants = Set.empty, variables = Set(varFor("x")), localCallables = Set.empty)
            )
          )
        )
      }
      test("modify UNWIND clause") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              unwind(listOfInt(1, 2, 3), varFor("x")),
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
      }
      test("modify whole query") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              statement,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
      }
    }

    withClue(
      """=====
        |DEFINED PROCEDURE proc() {
        |  RETURN 1 AS a, 2 AS b
        |}
        |CALL proc() YIELD a AS a
        |RETURN a AS a
        |=====
        |""".stripMargin
    ) {
      val statement = queryWithLocalDefinitions(
        localProcedureDefinition("proc").body(
          return_(aliasedReturnItem(literalInt(1), "a"), aliasedReturnItem(literalInt(2), "b"))
        )
      )(
        singleQuery(
          call(Seq.empty, "proc", yields = Some(Seq(varFor("a")))),
          return_(aliasedReturnItem(varFor("a")))
        )
      )
      test("modify local procedure definition return") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              return_(aliasedReturnItem(literalInt(1), "a"), aliasedReturnItem(literalInt(2), "b")),
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
      }
      test("modify local procedure definition return item") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              aliasedReturnItem(literalInt(2), "b"),
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
      }
    }

    withClue(
      """=====
        |RETURN 1 AS x
        |UNION
        |RETURN 1 AS x
        |=====
        |""".stripMargin
    ) {
      val firstReturn = return_(AliasedReturnItem(literalInt(1), varFor("x"))(InputPosition(1, 1, 1)))
      val secondReturn = return_(AliasedReturnItem(literalInt(1), varFor("x"))(InputPosition(2, 2, 2)))
      val statement = union(
        singleQuery(firstReturn),
        singleQuery(secondReturn)
      )
      test("modify subtrees only different by position — return in first arm but not in second") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              firstReturn,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "first"
            )
          )
        )
      }
      test("modify subtrees only different by position — return in second arm but not in first") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              secondReturn,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "second"
            )
          )
        )
      }
      test("modify subtrees only different by position — return in both arms") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              firstReturn,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "first"
            ),
            mod(
              secondReturn,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "second"
            )
          )
        )
      }
      test("modify subtrees only different by position — return in both arms — modification reversed") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              secondReturn,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "second"
            ),
            mod(
              firstReturn,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "first"
            )
          )
        )
      }
    }
  }

  withClue("cache gets not picked up due to no op modification") {
    /* Note that the difference between shouldPickUpCacheModifications and shouldNotPickUpCacheModifications
     * is whether the expected working scope gets modified or not.
     *
     * Regardless, no op modifications do not modify the expected working scope
     * but still added an entry to the cache in the test logic.
     *
     * Hence, shouldPickUpCacheModifications and shouldNotPickUpCacheModifications should both succeed on no op modifications.
     *
     * No op modifications simulates outdated cache entries.
     */

    withClue(
      """=====
        |UNWIND [1, 2, 3] AS x
        |MATCH (n {p: x})-[r:R]->()
        |RETURN x AS x
        |=====
        |""".stripMargin
    ) {
      val statement = singleQuery(
        unwind(listOfInt(1, 2, 3), varFor("x")),
        match_(
          relationshipChain(
            nodePat(Some("n"), properties = Some(mapOf("p" -> varFor("x")))),
            relPat(Some("r"), labelExpression = Some(labelRelTypeLeaf("R"))),
            nodePat()
          )
        ),
        return_(aliasedReturnItem(varFor("x")))
      )
      test("modify ast node not in the query") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 4, 6),
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
        shouldNotPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 4, 6),
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
      }
      test("modify ast node in the query but different incoming variables") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 2, 3),
              RegularContext(constants = Set.empty, variables = Set(varFor("foo")), localCallables = Set.empty)
            )
          )
        )
        shouldNotPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 2, 3),
              RegularContext(constants = Set.empty, variables = Set(varFor("foo")), localCallables = Set.empty)
            )
          )
        )
      }
      test("modify ast node in the query but different incoming constant") {
        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 2, 3),
              RegularContext(constants = Set(varFor("foo")), variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
        shouldNotPickUpCacheModifications(
          statement,
          modify(
            mod(
              listOfInt(1, 2, 3),
              RegularContext(constants = Set(varFor("foo")), variables = Set.empty, localCallables = Set.empty)
            )
          )
        )
      }
    }

    withClue(
      """=====
        |UNWIND [1, 2, 3] AS a
        |CALL (a) {
        |  // Incoming: conts: {a} vars: {}
        |  RETURN 1 AS x
        |}
        |// Incoming: conts: {} vars: {a}
        |RETURN 1 AS x
        |=====
        |""".stripMargin
    ) {
      val return1 =
        Return(ReturnItems(FreeProjection, Seq(aliasedReturnItem(literalInt(1), "x")))(pos))(InputPosition(1, 1, 1))
      val return2 =
        Return(ReturnItems(FreeProjection, Seq(aliasedReturnItem(literalInt(1), "x")))(pos))(InputPosition(2, 2, 2))
      val statement = singleQuery(
        unwind(listOfInt(1, 2, 3), varFor("a")),
        scopeClauseSubqueryCall(
          false,
          Seq(varFor("a")),
          return1
        ),
        return2
      )
      for {
        (constants, variables) <- Seq[(Set[LogicalVariable], Set[LogicalVariable])](
          (Set.empty, Set.empty),
          (Set.empty, Set(varFor("a"), varFor("b"))),
          (Set(varFor("a"), varFor("b")), Set.empty),
          (Set(varFor("x")), Set.empty),
          (Set.empty, Set(varFor("x"))),
          (Set(varFor("a")), Set(varFor("a"))),
          (Set(varFor("a")), Set(varFor("b"))),
          (Set(varFor("b")), Set(varFor("a"))),
          (Set(varFor("c")), Set(varFor("d")))
        )
      } {
        def toStr(set: Set[LogicalVariable]): String = set.map(_.name).mkString("{", ", ", "}")
        test(
          s"modify ast node in the query but different incoming | conts: ${toStr(constants)} vars: ${toStr(variables)}"
        ) {
          val mods = modify(
            mod(
              return1,
              RegularContext(constants, variables, Set.empty),
              "inner"
            ),
            mod(
              return2,
              RegularContext(constants, variables, Set.empty),
              "outer"
            )
          )
          shouldPickUpCacheModifications(statement, mods)
          shouldNotPickUpCacheModifications(statement, mods)
        }
      }
    }
  }

  withClue("cache correctness under identity-gated lookup") {
    withClue(
      """=====
        |UNWIND [1, 2, 3] AS x
        |RETURN x AS x
        |=====
        |""".stripMargin
    ) {
      val statement = singleQuery(
        unwind(listOfInt(1, 2, 3), varFor("x")),
        return_(aliasedReturnItem(varFor("x")))
      )

      test("mod keyed by a structurally-equal-but-identity-distinct node is treated as no-op") {
        val freshUnwind = unwind(listOfInt(1, 2, 3), varFor("x"))

        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              freshUnwind,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "fresh unwind"
            )
          )
        )
        shouldNotPickUpCacheModifications(
          statement,
          modify(
            mod(
              freshUnwind,
              RegularContext(constants = Set.empty, variables = Set.empty, localCallables = Set.empty),
              "fresh unwind"
            )
          )
        )
      }

      test("mod keyed by a structurally-equal-but-identity-distinct leaf is treated as no-op") {
        val freshItem = aliasedReturnItem(varFor("x"))

        shouldPickUpCacheModifications(
          statement,
          modify(
            mod(
              freshItem,
              RegularContext(constants = Set.empty, variables = Set(varFor("x")), localCallables = Set.empty),
              "fresh return item"
            )
          )
        )
        shouldNotPickUpCacheModifications(
          statement,
          modify(
            mod(
              freshItem,
              RegularContext(constants = Set.empty, variables = Set(varFor("x")), localCallables = Set.empty),
              "fresh return item"
            )
          )
        )
      }

      test("mod keyed by statement's own node with identity-distinct incoming variables is treated as no-op") {
        val statementsReturnItem = statement.folder.treeCollect {
          case item: org.neo4j.cypher.internal.ast.AliasedReturnItem => item
        }.head

        val incomingWithFreshX =
          RegularContext(
            constants = Set.empty,
            variables = Set(varFor("x")), // fresh `x` — same name, new instance
            localCallables = Set.empty
          )

        shouldPickUpCacheModifications(
          statement,
          modify(mod(statementsReturnItem, incomingWithFreshX, "identity-distinct incoming x"))
        )
        shouldNotPickUpCacheModifications(
          statement,
          modify(mod(statementsReturnItem, incomingWithFreshX, "identity-distinct incoming x"))
        )
      }
    }
  }
}
