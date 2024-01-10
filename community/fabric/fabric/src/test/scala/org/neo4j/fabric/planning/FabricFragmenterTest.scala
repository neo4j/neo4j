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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.FragmentTestUtils
import org.neo4j.fabric.ProcedureSignatureResolverTestSupport
import org.neo4j.fabric.planning.Fragment.Apply
import org.neo4j.fabric.planning.Fragment.Leaf
import org.neo4j.fabric.planning.Use.Declared
import org.neo4j.fabric.planning.Use.Inherited
import org.scalatest.Inside

class FabricFragmenterTest
    extends FabricTest
    with Inside
    with ProcedureSignatureResolverTestSupport
    with FragmentTestUtils
    with AstConstructionTestSupport {

  "USE handling: " - {

    "disallow USE inside fragment" in {
      the[SyntaxException]
        .thrownBy(
          fragment(
            """WITH 1 AS x
              |USE i
              |RETURN x
              |""".stripMargin
          )
        )
        .getMessage
        .should(include(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query."
        ))

    }

    "not propagate USE out" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  USE g
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].use.shouldEqual(defaultUse)
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(Declared(use("g")))
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .input
        .as[Fragment.Leaf]
        .use
        .shouldEqual(defaultUse)
    }

    "declared without imported variable with a nested subquery" in {
      val frag = fragment(
        """UNWIND mega.graphIds() as g
          |CALL {
          |  USE x
          |  CALL {
          |    MATCH (n) RETURN n
          |  }
          |  RETURN n
          |}
          |RETURN n
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].use.shouldEqual(defaultUse)
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .input
        .as[Fragment.Leaf]
        .use
        .shouldEqual(defaultUse)
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(Declared(use("x")))
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(Inherited(Declared(use("x")))(pos))
    }

    "declared with imported variable with a nested subquery" in {
      val frag = fragment(
        """UNWIND mega.graphIds() as g
          |CALL {
          |  WITH g
          |  USE x
          |  CALL {
          |    MATCH (n) RETURN n
          |  }
          |  RETURN n
          |}
          |RETURN n
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].use.shouldEqual(defaultUse)
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .input
        .as[Fragment.Leaf]
        .use
        .shouldEqual(defaultUse)
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(Declared(use("x")))
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(Inherited(Declared(use("x")))(pos))
    }

    "inherited from default in subquery" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].use.shouldEqual(defaultUse)
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(
          Inherited(defaultUse)(
            pos
          )
        )
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .input
        .as[Fragment.Leaf]
        .use
        .shouldEqual(defaultUse)
    }

    "inherited from declared in subquery" in {
      val frag = fragment(
        """USE g
          |WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].use.shouldEqual(Declared(use("g")))
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .inner
        .as[Fragment.Leaf]
        .use
        .shouldEqual(
          Inherited(
            Declared(use("g"))
          )(pos)
        )
      frag
        .as[Fragment.Leaf]
        .input
        .as[Fragment.Apply]
        .input
        .as[Fragment.Leaf]
        .use
        .shouldEqual(Declared(use("g")))
    }

    "disallow USE at start of non-initial fragment" in {
      the[SyntaxException]
        .thrownBy(
          fragment(
            """WITH 1 AS x
              |CALL {
              |  RETURN 2 AS y
              |}
              |USE i
              |RETURN x
              |""".stripMargin
          )
        )
        .getMessage
        .should(include(
          "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query."
        ))
    }

    "allow USE to reference outer variable" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  USE g(x)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin
      )

      inside(frag) {
        case Leaf(Apply(_, inner: Leaf, _), _, _) =>
          inner.use.shouldEqual(Declared(use(function("g", varFor("x")))))
      }
    }

    "allow USE to reference imported variable" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  WITH x
          |  USE g(x)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin
      )

      inside(frag) {
        case Leaf(Apply(_, inner: Leaf, _), _, _) =>
          inner.use.shouldEqual(Declared(use(function("g", varFor("x")))))
      }
    }

    "disallow USE to reference missing variable" in {

      the[SyntaxException]
        .thrownBy(
          fragment(
            """WITH 1 AS x
              |CALL {
              |  USE g(z)
              |  RETURN 2 AS y
              |}
              |RETURN x
              |""".stripMargin
          )
        )
        .getMessage
        .should(include("Variable `z` not defined"))
    }

    "disallow USE to reference outer variable after WITH" in {

      the[SyntaxException]
        .thrownBy(
          fragment(
            """WITH 1 AS x, 2 AS y
              |CALL {
              |  WITH x
              |  USE g(y)
              |  RETURN 2 AS z
              |}
              |RETURN z
              |""".stripMargin
          )
        )
        .getMessage
        .should(include("Variable `y` not defined"))
    }

    "InTransactionsParameters stored on apply" in {
      val frag = fragment(
        """
          |CALL {
          |  MATCH (n) RETURN n
          |} IN TRANSACTIONS
          |RETURN n
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].input.as[Fragment.Apply].inTransactionsParameters.isDefined.shouldBe(true)
    }

    "InTransactionsParameters stored on apply with USE" in {
      val frag = fragment(
        """
          |CALL {
          |  USE x
          |  MATCH (n) RETURN n
          |} IN TRANSACTIONS
          |RETURN n
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].input.as[Fragment.Apply].inTransactionsParameters.isDefined.shouldBe(true)
    }
  }

  "Full queries:" - {

    "plain query" in {
      fragment(
        """WITH 1 AS a
          |RETURN a
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(withLit(1, "a"), returnVars("a")), Seq("a"))
      )
    }

    "plain query with USE" in {
      fragment(
        """USE bar
          |WITH 1 AS a
          |RETURN a
          |""".stripMargin
      ).shouldEqual(
        init(Declared(use("bar")))
          .leaf(Seq(use("bar"), withLit(1, "a"), returnVars("a")), Seq("a"))
      )
    }

    "subquery with USE" in {
      fragment(
        """WITH 1 AS a
          |CALL {
          |  USE g
          |  RETURN 2 AS b
          |}
          |RETURN a, b
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(withLit(1, "a")), Seq("a"))
          .apply(_ =>
            init(Declared(use("g")), Seq("a"), Seq())
              .leaf(Seq(use("g"), returnLit(2 -> "b")), Seq("b"))
          )
          .leaf(Seq(returnVars("a", "b")), Seq("a", "b"))
      )
    }

    "correlated subquery" in {
      fragment(
        """WITH 1 AS a
          |CALL {
          |  WITH a
          |  RETURN a AS b
          |}
          |RETURN a, b
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(withLit(1, "a")), Seq("a"))
          .apply(use =>
            init(Inherited(use)(pos), Seq("a"), Seq("a"))
              .leaf(Seq(withVar("a"), returnAliased("a" -> "b")), Seq("b"))
          )
          .leaf(Seq(returnVars("a", "b")), Seq("a", "b"))
      )
    }

    "nested queries, with scoped USE and importing WITH" in {
      fragment(
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  CALL {
          |    USE foo
          |    RETURN 3 AS z
          |  }
          |  CALL {
          |    WITH y
          |    RETURN 4 AS w
          |  }
          |  RETURN w, y, z
          |}
          |RETURN x, w, y, z
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(withLit(1, "x")), Seq("x"))
          .apply(u =>
            init(Inherited(u)(pos), Seq("x"))
              .leaf(Seq(withLit(2, "y")), Seq("y"))
              .apply(_ =>
                init(Declared(use("foo")), Seq("y"))
                  .leaf(Seq(use("foo"), returnLit(3 -> "z")), Seq("z"))
              )
              .apply(u =>
                init(Inherited(u)(pos), Seq("y", "z"), Seq("y"))
                  .leaf(Seq(withVar("y"), returnLit(4 -> "w")), Seq("w"))
              )
              .leaf(Seq(returnVars("w", "y", "z")), Seq("w", "y", "z"))
          )
          .leaf(Seq(returnVars("x", "w", "y", "z")), Seq("x", "w", "y", "z"))
      )
    }

    "fragment with only USE clause" in {
      fragment(
        """USE foo
          |CALL {
          |  RETURN 1 AS a
          |}
          |RETURN a
          |""".stripMargin
      ).shouldEqual(
        init(Declared(use("foo")))
          .leaf(Seq(use("foo")), Seq())
          .apply(use =>
            init(Inherited(use)(pos))
              .leaf(Seq(returnLit(1 -> "a")), Seq("a"))
          )
          .leaf(Seq(returnVars("a")), Seq("a"))
      )
    }

    "union query, with different USE " in {
      fragment(
        """USE foo
          |RETURN 1 AS y
          |  UNION
          |USE bar
          |RETURN 2 AS y
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse).union(
          init(Declared(use("foo"))).leaf(Seq(use("foo"), returnLit(1 -> "y")), Seq("y")),
          init(Declared(use("bar"))).leaf(Seq(use("bar"), returnLit(2 -> "y")), Seq("y"))
        )
      )
    }

    "nested union query, with USE and importing WITH" in {
      fragment(
        """WITH 1 AS x
          |CALL {
          |  USE foo
          |  RETURN 1 AS y
          |    UNION
          |  WITH x
          |  RETURN 2 AS y
          |}
          |RETURN x, y
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(withLit(1, "x")), Seq("x"))
          .apply(u =>
            init(Inherited(defaultUse)(pos), Seq("x")).union(
              init(Declared(use("foo")), Seq("x"))
                .leaf(Seq(use("foo"), returnLit(1 -> "y")), Seq("y")),
              init(Inherited(u)(pos), Seq("x"), Seq("x"))
                .leaf(Seq(withVar("x"), returnLit(2 -> "y")), Seq("y"))
            )
          )
          .leaf(Seq(returnVars("x", "y")), Seq("x", "y"))
      )
    }

    "subquery calling procedure" in {
      fragment(
        """WITH 1 AS x
          |CALL {
          |  USE g
          |  CALL some.procedure() YIELD z, y
          |  RETURN z, y
          |}
          |RETURN x, y, z
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(withLit(1, "x")), Seq("x"))
          .apply(_ =>
            init(Declared(use("g")), Seq("x"))
              .leaf(
                Seq(
                  use("g"),
                  call(Seq("some"), "procedure", yields = Some(Seq(varFor("z"), varFor("y")))),
                  returnVars("z", "y")
                ),
                Seq("z", "y")
              )
          )
          .leaf(Seq(returnVars("x", "y", "z")), Seq("x", "y", "z"))
      )
    }

    "no output columns for unnamed variables" in {
      val frag = fragment(
        """WITH 1 AS x
          |MATCH ()
          |CALL {
          |  RETURN 1 AS y
          |}
          |RETURN x, y
          |""".stripMargin
      )

      frag
        .as[Fragment.Segment]
        .input
        .as[Fragment.Segment]
        .input
        .outputColumns
        .shouldEqual(Seq("x"))
    }

    "Variable with literal name" in {
      fragment(
        """MATCH (n)
          |WITH n AS `true`
          |RETURN `true`
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              match_(NodePattern(Some(varFor("n")), None, None, None)(pos)),
              with_(varFor("n").as("true")),
              returnVars("true")
            ),
            Seq("true")
          )
      )
    }

    "Literal with variable with same name in scope" in {
      fragment(
        """MATCH (n)
          |WITH n AS `true`
          |RETURN true
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              match_(NodePattern(Some(varFor("n")), None, None, None)(pos)),
              with_(varFor("n").as("true")),
              returnLit(true -> "true")
            ),
            Seq("true")
          )
      )
    }

    "Last clause is an update clause" in {
      fragment(
        """MATCH (n)
          |CREATE (m)
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              match_(NodePattern(Some(varFor("n")), None, None, None)(pos)),
              create(NodePattern(Some(varFor("m")), None, None, None)(pos))
            ),
            Seq.empty
          )
      )
    }

    val inTransactionParameters = Some(InTransactionsParameters(None, None, None)(pos))

    "Call in tx" in {
      fragment(
        """
          |CALL {
          |  MATCH (n)
          |  RETURN n
          |} IN TRANSACTIONS
          |RETURN n
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .apply(
            oldUse =>
              init(Inherited(oldUse)(pos))
                .leaf(
                  Seq(
                    match_(NodePattern(Some(varFor("n")), None, None, None)(pos)),
                    returnVars("n")
                  ),
                  Seq("n")
                ),
            inTransactionParameters
          )
          .leaf(Seq(returnVars("n")), Seq("n"))
      )
    }

    "Call in tx with use" in {
      fragment(
        """
          |CALL {
          |  USE x
          |  MATCH (n)
          |  RETURN n
          |} IN TRANSACTIONS
          |RETURN n
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .apply(
            _ =>
              init(Declared(use("x")))
                .leaf(
                  Seq(
                    use("x"),
                    match_(NodePattern(Some(varFor("n")), None, None, None)(pos)),
                    returnVars("n")
                  ),
                  Seq("n")
                ),
            inTransactionParameters
          )
          .leaf(Seq(returnVars("n")), Seq("n"))
      )
    }
  }

  "Procedures:" - {

    "a single known procedure local query" in {
      fragment(
        """CALL my.ns.myProcedure()
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              resolved(
                call(
                  Seq("my", "ns"),
                  "myProcedure",
                  Some(Seq()),
                  Some(Seq(varFor("a"), varFor("b")))
                )
              ),
              returnVars("a", "b")
            ),
            Seq("a", "b")
          )
      )
    }

    "a single known procedure local query without args" in {
      fragment(
        """CALL my.ns.myProcedure
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              resolved(
                call(
                  Seq("my", "ns"),
                  "myProcedure",
                  Some(Seq()),
                  Some(Seq(varFor("a"), varFor("b")))
                )
              ),
              returnVars("a", "b")
            ),
            Seq("a", "b")
          )
      )
    }

    "a single known procedure local query with args" in {
      fragment(
        """CALL my.ns.myProcedure2(1)
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              resolved(
                call(
                  Seq("my", "ns"),
                  "myProcedure2",
                  Some(Seq(literal(1))),
                  Some(Seq(varFor("a"), varFor("b")))
                )
              ),
              returnVars("a", "b")
            ),
            Seq("a", "b")
          )
      )
    }

    "a known procedure local query after a MATCH" in {
      fragment(
        """MATCH (n)
          |CALL my.ns.unitProcedure()
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              match_(NodePattern(Some(varFor("n")), None, None, None)(pos)),
              resolved(call(Seq("my", "ns"), "unitProcedure", Some(Seq()), None))
            ),
            Seq.empty
          )
      )
    }

    "an unknown procedure local query" in {
      fragment(
        """CALL unknownProcedure() YIELD x, y
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(call(Seq(), "unknownProcedure", Some(Seq()), Some(Seq(varFor("x"), varFor("y"))))),
            Seq("x", "y")
          )
      )
    }

    "a known function" in {
      fragment(
        """RETURN const0() AS x
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(return_(resolved(function("const0")).as("x"))), Seq("x"))
      )
    }

    "a known function with namespace and args" in {
      fragment(
        """RETURN my.ns.const0(1) AS x
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(
            Seq(
              return_(
                resolved(function(Seq("my", "ns"), "const0", literal(1))).as("x")
              )
            ),
            Seq("x")
          )
      )
    }

    "an unknown function" in {
      fragment(
        """RETURN my.unknown() AS x
          |""".stripMargin
      ).shouldEqual(
        init(defaultUse)
          .leaf(Seq(return_(function(Seq("my"), "unknown").as("x"))), Seq("x"))
      )
    }
  }

  "Input position" - {
    "Single query" in {
      val frag = fragment(
        """RETURN 1 AS x
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].pos.shouldEqual(InputPosition(0, 1, 1))
    }

    "Union query" in {
      val frag = fragment(
        """RETURN 1 AS x
          |UNION
          |RETURN 2 AS x
          |""".stripMargin
      )

      frag.as[Fragment.Union].pos.shouldEqual(InputPosition(14, 2, 1))
      frag.as[Fragment.Union].lhs.as[Fragment.Leaf].pos.shouldEqual(InputPosition(0, 1, 1))
      frag.as[Fragment.Union].rhs.as[Fragment.Leaf].pos.shouldEqual(InputPosition(20, 3, 1))
    }

    "Subquery" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  RETURN y
          |}
          |RETURN x, y
          |""".stripMargin
      )

      frag.as[Fragment.Leaf].pos.shouldEqual(InputPosition(46, 6, 1))
      frag.as[Fragment.Leaf].input.as[Fragment.Apply].pos.shouldEqual(InputPosition(12, 2, 1))
      frag.as[Fragment.Leaf].input.as[Fragment.Apply].inner.pos.shouldEqual(InputPosition(21, 3, 3))
      frag.as[Fragment.Leaf].input.as[Fragment.Apply].input.pos.shouldEqual(InputPosition(0, 1, 1))
    }
  }

  private def withLit(num: Any, varName: String): With =
    with_(literal(num).as(varName))

  private def withVar(varName: String): With =
    with_(varFor(varName).as(varName))

  private def returnAliased(vars: (String, String)*) =
    return_(vars.map(v => varFor(v._1).as(v._2)): _*)

  private def resolved(unresolved: UnresolvedCall): ResolvedCall =
    ResolvedCall(signatures.procedureSignature)(unresolved)

  private def resolved(unresolved: FunctionInvocation): ResolvedFunctionInvocation =
    ResolvedFunctionInvocation(signatures.functionSignature)(unresolved)
}
