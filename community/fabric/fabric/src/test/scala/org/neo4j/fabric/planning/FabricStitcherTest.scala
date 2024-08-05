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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.SyntaxException
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.FragmentTestUtils
import org.neo4j.fabric.ProcedureSignatureResolverTestSupport
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.planning.Fragment.Apply
import org.neo4j.fabric.planning.Use.Declared
import org.neo4j.fabric.planning.Use.Inherited
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.scalatest.Inside

import java.util.UUID

class FabricStitcherTest
    extends FabricTest
    with Inside
    with ProcedureSignatureResolverTestSupport
    with FragmentTestUtils
    with AstConstructionTestSupport {

  private def importParams(names: String*) =
    with_(names.map(v => parameter(Columns.paramName(v), ct.any).as(v)): _*)

  private val dummyQuery = ""
  private val dummyPipeline = pipeline("RETURN 1")

  "Single-graph:" - {

    def stitching(fragment: Fragment, callInTransactionsEnabled: Boolean = false) =
      FabricStitcher(
        dummyQuery,
        compositeContext = false,
        dummyPipeline,
        new UseHelper(Catalog.empty, defaultGraphName),
        callInTransactionsEnabled
      )
        .convert(fragment).withoutLocalAndRemote

    "single fragment" in {
      stitching(
        init(defaultUse).leaf(Seq(return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse).exec(singleQuery(return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "single fragment, with USE" in {
      stitching(
        init(defaultUse).leaf(Seq(use("foo"), return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse).exec(singleQuery(return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "single fragments with imports" in {
      stitching(
        init(defaultUse, Seq("x", "y"), Seq("y")).leaf(Seq(return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse, Seq("x", "y"), Seq("y")).exec(
          singleQuery(importParams("y"), return_(literal(1).as("a"))),
          Seq("a")
        )
      )
    }

    "single fragment standalone call" in {
      stitching(
        init(defaultUse).leaf(Seq(call(Seq("my"), "proc")), Seq())
      ).shouldEqual(
        init(defaultUse).exec(singleQuery(call(Seq("my"), "proc")), Seq())
      )
    }

    "nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u =>
            init(Inherited(u)(pos), Seq("a"))
              .leaf(Seq(return_(literal(2).as("b"))), Seq("b"))
          )
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(
            singleQuery(
              with_(literal(1).as("a")),
              scopeClauseSubqueryCall(false, Seq.empty, return_(literal(2).as("b"))),
              return_(literal(3).as("c"))
            ),
            Seq("c")
          )
      )
    }

    "nested fragment with nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u =>
            init(Inherited(u)(pos), Seq("a"))
              .leaf(Seq(with_(literal(2).as("b"))), Seq("b"))
              .apply(u =>
                init(Inherited(u)(pos), Seq("b"))
                  .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
              )
              .leaf(Seq(return_(literal(4).as("d"))), Seq("d"))
          )
          .leaf(Seq(return_(literal(5).as("e"))), Seq("e"))
      ).shouldEqual(
        init(defaultUse)
          .exec(
            singleQuery(
              with_(literal(1).as("a")),
              scopeClauseSubqueryCall(
                false,
                Seq.empty,
                with_(literal(2).as("b")),
                scopeClauseSubqueryCall(false, Seq.empty, return_(literal(3).as("c"))),
                return_(literal(4).as("d"))
              ),
              return_(literal(5).as("e"))
            ),
            Seq("e")
          )
      )
    }

    "nested fragment after nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u =>
            init(Inherited(u)(pos), Seq("a"))
              .leaf(Seq(return_(literal(2).as("b"))), Seq("b"))
          )
          .apply(u =>
            init(Inherited(u)(pos), Seq("a", "b"))
              .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
          )
          .leaf(Seq(return_(literal(4).as("d"))), Seq("d"))
      ).shouldEqual(
        init(defaultUse)
          .exec(
            singleQuery(
              with_(literal(1).as("a")),
              scopeClauseSubqueryCall(false, Seq.empty, return_(literal(2).as("b"))),
              scopeClauseSubqueryCall(false, Seq.empty, return_(literal(3).as("c"))),
              return_(literal(4).as("d"))
            ),
            Seq("d")
          )
      )
    }

    "nested fragment directly after USE" in {
      stitching(
        init(Declared(use("foo")))
          .leaf(Seq(use("foo")), Seq())
          .apply(u =>
            init(Inherited(u)(pos), Seq())
              .leaf(Seq(return_(literal(2).as("b"))), Seq("b"))
          )
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(Declared(use("foo")))
          .exec(
            singleQuery(
              scopeClauseSubqueryCall(false, Seq.empty, return_(literal(2).as("b"))),
              return_(literal(3).as("c"))
            ),
            Seq("c")
          )
      )
    }

    "union fragment, different imports" in {
      stitching(
        init(defaultUse, Seq("x", "y", "z"))
          .union(
            init(defaultUse, Seq("x", "y", "z"), Seq("y"))
              .leaf(Seq(return_(literal(1).as("a"))), Seq("a")),
            init(defaultUse, Seq("x", "y", "z"), Seq("z"))
              .leaf(Seq(return_(literal(2).as("a"))), Seq("a"))
          )
      ).shouldEqual(
        init(defaultUse, Seq("x", "y", "z"), Seq("y", "z"))
          .exec(
            union(
              singleQuery(importParams("y"), return_(literal(1).as("a"))),
              singleQuery(importParams("z"), return_(literal(2).as("a")))
            ),
            Seq("a")
          )
      )
    }

    "nested union" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("x"), literal(2).as("y"), literal(3).as("z"))), Seq("x", "y", "z"))
          .apply(u =>
            init(Inherited(u)(pos), Seq("x", "y", "z"))
              .union(
                init(defaultUse, Seq("x", "y", "z"), Seq("y"))
                  .leaf(Seq(with_(varFor("y").as("y")), return_(varFor("y").as("a"))), Seq("a")),
                init(defaultUse, Seq("x", "y", "z"), Seq("z"))
                  .leaf(Seq(with_(varFor("z").as("z")), return_(varFor("z").as("a"))), Seq("a"))
              )
          )
          .leaf(Seq(return_(literal(4).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(
            singleQuery(
              with_(literal(1).as("x"), literal(2).as("y"), literal(3).as("z")),
              scopeClauseSubqueryCall(
                false,
                Seq(varFor("y"), varFor("z")),
                union(
                  singleQuery(with_(varFor("y").as("y")), return_(varFor("y").as("a"))),
                  singleQuery(with_(varFor("z").as("z")), return_(varFor("z").as("a")))
                )
              ),
              return_(literal(4).as("c"))
            ),
            Seq("c")
          )
      )
    }
  }

  "Multi-graph:" - {
    val fabricRef = new DatabaseReferenceImpl.Composite(
      new NormalizedDatabaseName(defaultGraphName),
      DatabaseIdFactory.from(defaultGraphName, UUID.randomUUID()),
      java.util.Set.of()
    )
    val catalog =
      Catalog.byQualifiedName(Seq(Catalog.Composite(0, fabricRef)))

    def stitching(fragment: Fragment, callInTransactionsEnabled: Boolean = false) =
      FabricStitcher(
        dummyQuery,
        compositeContext = true,
        dummyPipeline,
        new UseHelper(catalog, defaultGraphName),
        callInTransactionsEnabled
      )
        .convert(fragment).withoutLocalAndRemote

    "rewrite" in {
      import org.neo4j.fabric.util.Rewritten.RewritingOps

      val tree =
        init(defaultUse)
          .apply(_ => init(defaultUse))

      tree.rewritten.topDown {
        case i: Fragment.Init => i.copy(argumentColumns = Seq("a"))
      }
    }

    "nested fragment, different USE" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(_ =>
            init(Declared(use("foo")), Seq("a"))
              .leaf(Seq(use("foo"), return_(literal(2).as("b"))), Seq("b"))
          )
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(singleQuery(with_(literal(1).as("a")), return_(varFor("a").as("a"))), Seq("a"))
          .apply(_ =>
            init(Declared(use("foo")), Seq("a"))
              .exec(singleQuery(return_(literal(2).as("b"))), Seq("b"))
          )
          .exec(singleQuery(input(varFor("a"), varFor("b")), return_(literal(3).as("c"))), Seq("c"))
      )
    }

    "nested fragment, different USE, with imports" in {
      /*
      WITH 1 as a
      CALL {
        WITH a as a
        USE foo
        RETURN 2 as b
      }
      RETURN 3 as c
       */
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(_ =>
            init(Declared(use("foo")), Seq("a"), Seq("a"))
              .leaf(Seq(with_(varFor("a").as("a")), use("foo"), return_(literal(2).as("b"))), Seq("b"))
          )
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(singleQuery(with_(literal(1).as("a")), return_(varFor("a").as("a"))), Seq("a"))
          .apply(_ =>
            init(Declared(use("foo")), Seq("a"), Seq("a"))
              .exec(
                singleQuery(
                  with_(parameter("@@a", ct.any).as("a")),
                  with_(varFor("a").as("a")),
                  return_(literal(2).as("b"))
                ),
                Seq("b")
              )
          )
          .exec(singleQuery(input(varFor("a"), varFor("b")), return_(literal(3).as("c"))), Seq("c"))
      )
    }

    "call in transactions" - {
      val inTransactionParameters = Some(InTransactionsParameters(None, None, None, None)(pos))

      "unwind added and with clause not added for literals" in {
        /*
        CALL {
          USE foo
          RETURN 1 as a
        } IN TRANSACTIONS
        RETURN 2 as b
        =>
        (UNWIND $rows as row
        CALL {
          USE foo
          RETURN 1 as a
        } IN TRANSACTIONS
        RETURN a
        )
        (RETURN 2 as b)
         */
        val actual = stitching(
          init(defaultUse)
            .apply(
              _ =>
                init(Declared(use("foo")), Seq(), Seq())
                  .leaf(
                    Seq(
                      use("foo"),
                      return_(literal(1).as("a"))
                    ),
                    Seq("a")
                  ),
              inTransactionParameters
            )
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")),
          callInTransactionsEnabled = true
        )
        val expected = init(defaultUse)
          .apply(
            _ =>
              init(Declared(use("foo")), Seq(), Seq())
                .exec(
                  singleQuery(
                    unwind(
                      parameter(Apply.CALL_IN_TX_ROWS, CTAny),
                      varFor(Apply.CALL_IN_TX_ROW)
                    ),
                    with_(prop(Apply.CALL_IN_TX_ROW, Apply.CALL_IN_TX_ROW_ID).as(Apply.CALL_IN_TX_ROW_ID)),
                    scopeClauseSubqueryCallInTransactions(
                      false,
                      Seq.empty,
                      InTransactionsParameters(None, None, None, None)(pos),
                      return_(literal(1).as("a"))
                    ),
                    return_(varFor("a").as("a"), varFor(Apply.CALL_IN_TX_ROW_ID).as(Apply.CALL_IN_TX_ROW_ID))
                  ),
                  Seq("a", Apply.CALL_IN_TX_ROW_ID)
                ),
            inTransactionParameters
          )
          .exec(singleQuery(input(varFor("a")), return_(literal(2).as("b"))), Seq("b"))
        actual.shouldEqual(expected)
      }

      "with clause added" in {
        /*
        WITH 1 as b
        CALL {
          WITH b
          USE foo
          RETURN b as c
        } IN TRANSACTIONS
        RETURN 1 as d
        =>
        (WITH 1 as b)
        (UNWIND $rows as row
        USE foo
        WITH row
        WITH row.b as b
        CALL {
          WITH b
          RETURN b as c
        } IN TRANSACTIONS
        RETURN c
        )
        (RETURN 1 as d)
         */
        val actual = stitching(
          init(defaultUse)
            .leaf(Seq(with_(literal(1).as("b"))), Seq("b"))
            .apply(
              _ =>
                init(Declared(use("foo")), Seq("b"), Seq("b"))
                  .leaf(
                    Seq(
                      with_(varFor("b").as("b")),
                      use("foo"),
                      return_(varFor("b").as("c"))
                    ),
                    Seq("c")
                  ),
              inTransactionParameters
            )
            .leaf(Seq(return_(literal(1).as("d"))), Seq("d")),
          true
        )
        val expected = init(defaultUse)
          .exec(
            singleQuery(
              with_(literal(1).as("b")),
              return_(varFor("b").as("b"))
            ),
            Seq("b")
          )
          .apply(
            _ =>
              init(Declared(use("foo")), Seq("b"), Seq("b"))
                .exec(
                  singleQuery(
                    unwind(
                      parameter(Apply.CALL_IN_TX_ROWS, CTAny),
                      varFor(Apply.CALL_IN_TX_ROW)
                    ),
                    with_(
                      prop(Apply.CALL_IN_TX_ROW, "b").as("b"),
                      prop(Apply.CALL_IN_TX_ROW, Apply.CALL_IN_TX_ROW_ID).as(Apply.CALL_IN_TX_ROW_ID)
                    ),
                    scopeClauseSubqueryCallInTransactions(
                      false,
                      Seq(varFor("b")),
                      InTransactionsParameters(None, None, None, None)(pos),
                      with_(varFor("b").as("b")),
                      return_(varFor("b").as("c"))
                    ),
                    return_(varFor("c").as("c"), varFor(Apply.CALL_IN_TX_ROW_ID).as(Apply.CALL_IN_TX_ROW_ID))
                  ),
                  Seq("c", Apply.CALL_IN_TX_ROW_ID)
                ),
            inTransactionParameters
          )
          .exec(singleQuery(input(varFor("b"), varFor("c")), return_(literal(1).as("d"))), Seq("d"))
        actual.shouldEqual(
          expected
        )
      }
    }

    "errors" - {
      val inTransactionParameters = Some(InTransactionsParameters(None, None, None, None)(pos))

      "disallows call in transactions as apply" in {
        /*
        WITH 1 as a
        CALL {
          WITH a as a
          USE foo
          RETURN 2 as b
        } IN TRANSACTIONS
        RETURN 3 as c
         */
        val e = the[SyntaxException].thrownBy(
          stitching(
            init(defaultUse)
              .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
              .apply(
                _ =>
                  init(Declared(use("foo")), Seq("a"), Seq("a"))
                    .leaf(Seq(with_(varFor("a").as("a")), use("foo"), return_(literal(2).as("b"))), Seq("b")),
                inTransactionParameters
              )
              .leaf(Seq(return_(literal(3).as("c"))), Seq("c")),
            callInTransactionsEnabled = false
          )
        )

        e.getMessage.should(
          include("Transactional subquery is not allowed here. This feature is not supported on composite databases.")
        )
      }

      "disallows call in transactions as nested apply" in {
        // Think this case is illegal in cypher anyway due to
        // org.neo4j.exceptions.SyntaxException: CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported
        val e = the[SyntaxException].thrownBy(
          stitching(
            init(defaultUse)
              .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
              .apply(oldUse =>
                init(Inherited(oldUse)(pos), Seq("a"), Seq("a"))
                  .apply(
                    _ =>
                      init(Declared(use("foo")), Seq("a"), Seq("a"))
                        .leaf(Seq(with_(varFor("a").as("a")), use("foo"), return_(literal(2).as("b"))), Seq("b")),
                    inTransactionParameters
                  )
              )
              .leaf(Seq(return_(literal(3).as("c"))), Seq("c")),
            callInTransactionsEnabled = false
          )
        )

        e.getMessage.should(
          include("Transactional subquery is not allowed here. This feature is not supported on composite databases.")
        )
      }

      "disallows call in transactions as subquery call" in {
        val e = the[SyntaxException].thrownBy(
          stitching(
            init(defaultUse)
              .leaf(
                Seq(with_(literal(1).as("a")), importingWithSubqueryCallInTransactions(create(nodePat(Some("n"))))),
                Seq("a")
              ),
            callInTransactionsEnabled = false
          )
        )

        e.getMessage.should(
          include("Transactional subquery is not allowed here. This feature is not supported on composite databases.")
        )
      }

      "disallows scoped call in transactions as subquery call" in {
        val e = the[SyntaxException].thrownBy(
          stitching(
            init(defaultUse)
              .leaf(
                Seq(
                  with_(literal(1).as("a")),
                  scopeClauseSubqueryCallInTransactions(
                    false,
                    Seq.empty,
                    inTransactionsParameters(None, None, None, None),
                    create(nodePat(Some("n")))
                  )
                ),
                Seq("a")
              ),
            callInTransactionsEnabled = false
          )
        )

        e.getMessage.should(
          include("Transactional subquery is not allowed here. This feature is not supported on composite databases.")
        )
      }
    }
  }
}
