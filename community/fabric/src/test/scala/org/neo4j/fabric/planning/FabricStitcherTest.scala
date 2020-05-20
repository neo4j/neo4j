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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.FragmentTestUtils
import org.neo4j.fabric.ProcedureSignatureResolverTestSupport
import org.neo4j.fabric.planning.Use.Declared
import org.neo4j.fabric.planning.Use.Inherited
import org.scalatest.Inside

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

    def stitching(fragment: Fragment) =
      FabricStitcher(dummyQuery, allowMultiGraph = false, None, None, dummyPipeline)
        .convert(fragment).withoutLocalAndRemote


    "single fragment" in {
      stitching(
        init(defaultUse).leaf(Seq(return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse).exec(query(return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "single fragment, with USE" in {
      stitching(
        init(defaultUse).leaf(Seq(use("foo"), return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse).exec(query(return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "single fragments with imports" in {
      stitching(
        init(defaultUse, Seq("x", "y"), Seq("y")).leaf(Seq(return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse, Seq("x", "y"), Seq("y")).exec(
          query(importParams("y"), return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "single fragment standalone call" in {
      stitching(
        init(defaultUse).leaf(Seq(call(Seq("my"), "proc")), Seq())
      ).shouldEqual(
        init(defaultUse).exec(query(call(Seq("my"), "proc")), Seq())
      )
    }

    "nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u => init(Inherited(u)(pos), Seq("a"))
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("a")),
            subQuery(return_(literal(2).as("b"))),
            return_(literal(3).as("c"))
          ), Seq("c"))
      )
    }

    "nested fragment with nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u => init(Inherited(u)(pos), Seq("a"))
            .leaf(Seq(with_(literal(2).as("b"))), Seq("b"))
            .apply(u => init(Inherited(u)(pos), Seq("b"))
              .leaf(Seq(return_(literal(3).as("c"))), Seq("c")))
            .leaf(Seq(return_(literal(4).as("d"))), Seq("d")))
          .leaf(Seq(return_(literal(5).as("e"))), Seq("e"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("a")),
            subQuery(
              with_(literal(2).as("b")),
              subQuery(return_(literal(3).as("c"))),
              return_(literal(4).as("d"))
            ),
            return_(literal(5).as("e"))
          ), Seq("e"))
      )
    }

    "nested fragment after nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u => init(Inherited(u)(pos), Seq("a"))
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")))
          .apply(u => init(Inherited(u)(pos), Seq("a", "b"))
            .leaf(Seq(return_(literal(3).as("c"))), Seq("c")))
          .leaf(Seq(return_(literal(4).as("d"))), Seq("d"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("a")),
            subQuery(return_(literal(2).as("b"))),
            subQuery(return_(literal(3).as("c"))),
            return_(literal(4).as("d"))
          ), Seq("d"))
      )
    }

    "nested fragment directly after USE" in {
      stitching(
        init(Declared(use("foo")))
          .leaf(Seq(use("foo")), Seq())
          .apply(u => init(Inherited(u)(pos), Seq())
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(Declared(use("foo")))
          .exec(query(
            subQuery(return_(literal(2).as("b"))),
            return_(literal(3).as("c"))
          ), Seq("c"))
      )
    }

    "union fragment, different imports" in {
      stitching(
        init(defaultUse, Seq("x", "y", "z"))
          .union(
            init(defaultUse, Seq("x", "y", "z"), Seq("y"))
              .leaf(Seq(return_(literal(1).as("a"))), Seq("a")),
            init(defaultUse, Seq("x", "y", "z"), Seq("z"))
              .leaf(Seq(return_(literal(2).as("a"))), Seq("a")))
      ).shouldEqual(
        init(defaultUse, Seq("x", "y", "z"), Seq("y", "z"))
          .exec(query(union(
            singleQuery(importParams("y"), return_(literal(1).as("a"))),
            singleQuery(importParams("z"), return_(literal(2).as("a")))
          )), Seq("a"))
      )
    }

    "nested union" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("x"), literal(2).as("y"), literal(3).as("z"))), Seq("x", "y", "z"))
          .apply(u => init(Inherited(u)(pos), Seq("x", "y", "z"))
            .union(
              init(defaultUse, Seq("x", "y", "z"), Seq("y"))
                .leaf(Seq(with_(varFor("y").as("y")), return_(varFor("y").as("a"))), Seq("a")),
              init(defaultUse, Seq("x", "y", "z"), Seq("z"))
                .leaf(Seq(with_(varFor("z").as("z")), return_(varFor("z").as("a"))), Seq("a"))))
          .leaf(Seq(return_(literal(4).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("x"), literal(2).as("y"), literal(3).as("z")),
            subQuery(union(
              singleQuery(with_(varFor("y").as("y")), return_(varFor("y").as("a"))),
              singleQuery(with_(varFor("z").as("z")), return_(varFor("z").as("a")))
            )),
            return_(literal(4).as("c"))
          ), Seq("c"))
      )
    }
  }

  "Multi-graph:" - {

    def stitching(fragment: Fragment) =
      FabricStitcher(dummyQuery, allowMultiGraph = true, Some(defaultGraphName), None, dummyPipeline)
        .convert(fragment).withoutLocalAndRemote

    "nested fragment, different USE" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"))
            .leaf(Seq(use("foo"), return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(with_(literal(1).as("a")), return_(varFor("a").as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"))
            .exec(query(return_(literal(2).as("b"))), Seq("b")))
          .exec(query(input(varFor("a"), varFor("b")), return_(literal(3).as("c"))), Seq("c"))
      )
    }

    "nested fragment, different USE, with imports" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"), Seq("a"))
            .leaf(Seq(with_(varFor("a").as("a")), use("foo"), return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(with_(literal(1).as("a")), return_(varFor("a").as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"), Seq("a"))
            .exec(query(with_(parameter("@@a", ct.any).as("a")), with_(varFor("a").as("a")), return_(literal(2).as("b"))), Seq("b")))
          .exec(query(input(varFor("a"), varFor("b")), return_(literal(3).as("c"))), Seq("c"))
      )
    }
  }
}
