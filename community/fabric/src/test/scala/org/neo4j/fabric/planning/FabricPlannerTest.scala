/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.PeriodicCommitHint
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.fabric.FabricTest
import org.neo4j.fabric.FragmentTestUtils
import org.neo4j.fabric.ProcedureSignatureResolverTestSupport
import org.neo4j.fabric.cache.FabricQueryCache
import org.neo4j.fabric.config.FabricConfig
import org.neo4j.fabric.planning.FabricPlan.DebugOptions
import org.neo4j.fabric.util.Folded.Descend
import org.neo4j.fabric.util.Folded.FoldableOps
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.string.UTF8
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.prop.TableDrivenPropertyChecks

import java.time.Duration
import java.util.Optional

import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class FabricPlannerTest
    extends FabricTest
    with TableDrivenPropertyChecks
    with ProcedureSignatureResolverTestSupport
    with FragmentTestUtils
    with AstConstructionTestSupport {

  private def makeConfig(fabricDbName: String) =
    new FabricConfig(() => Duration.ZERO, new FabricConfig.DataStream(0, 0, 0, 0), false, true) {

      override def getFabricDatabaseName: Optional[NormalizedDatabaseName] =
        Optional.of(new NormalizedDatabaseName(fabricDbName))
    }

  private val config = makeConfig("mega")
  private val planner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

  private def instance(
    query: String,
    params: MapValue = params,
    fabricContext: Boolean = false
  ): planner.PlannerInstance =
    planner.instance(query, params, defaultGraphName).withForceFabricContext(fabricContext)

  private def plan(query: String, params: MapValue = params, fabricContext: Boolean = false) =
    instance(query, params, fabricContext).plan

  private def asRemote(
    query: String,
    partSelector: Fragment => Fragment.Exec = _.as[Fragment.Exec],
    fabricContext: Boolean = false
  ) = {
    val inst = instance(query, fabricContext = fabricContext)
    inst.asRemote(partSelector(inst.plan.query))
  }

  "asRemote: " - {

    "single query" in {
      val remote = asRemote(
        """RETURN 1 AS x
          |""".stripMargin
      )

      parse(remote.query).as[Query].part.as[SingleQuery].clauses
        .shouldEqual(Seq(
          return_(literal(1).as("x"))
        ))
    }

    "single query with USE" in {
      val remote = asRemote(
        """USE foo
          |RETURN 1 AS x
          |""".stripMargin
      )

      parse(remote.query).as[Query].part.as[SingleQuery].clauses
        .shouldEqual(Seq(
          return_(literal(1).as("x"))
        ))
    }

    "single schema command with USE" in {
      val remote = asRemote(
        """USE foo
          |CREATE INDEX myIndex FOR (n:Label) ON (n.prop)
          |""".stripMargin
      )

      parse(remote.query)
        .shouldEqual(
          CreateRangeNodeIndex(
            varFor("n"),
            labelName("Label"),
            List(prop("n", "prop")),
            Some("myIndex"),
            IfExistsThrowError,
            NoOptions,
            fromDefault = true
          )(pos)
        )
    }

    "periodic commit" in {
      val remote = asRemote(
        """USING PERIODIC COMMIT 200
          |LOAD CSV FROM 'someurl' AS line
          |CREATE (n)
          |RETURN line
          |""".stripMargin
      )

      parse(remote.query)
        .shouldEqual(
          Query(
            Some(PeriodicCommitHint(Some(literalUnsignedInt(200)))(pos)),
            singleQuery(
              LoadCSV(withHeaders = false, literal("someurl"), varFor("line"), None)(pos),
              create(nodePat("n")),
              return_(varFor("line").as("line"))
            )
          )(pos)
        )
    }

    "single admin command" in {
      val remote = asRemote(
        """CREATE ROLE myRole
          |""".stripMargin
      )

      parse(remote.query)
        .shouldEqual(CreateRole(Left("myRole"), None, IfExistsThrowError)(pos))
    }

    "single admin command with password" in {
      val remote = asRemote(
        """CREATE USER myUser SET PASSWORD 'secret'
          |""".stripMargin
      )

      remote.query
        .should(not(include("*")))

      parse(remote.query).as[CreateUser].initialPassword
        .should(matchPattern { case _: SensitiveParameter => })

      remote.extractedLiterals.values.toSeq.head
        .shouldEqual(UTF8.encode("secret"))
    }

    "single security procedure with password with USE" in {
      val remote = asRemote(
        """USE system
          |CALL dbms.security.createUser('myProcedureUser', 'secret')
          |""".stripMargin
      )

      remote.query
        .should(not(include("*")))

      parse(remote.query).as[Query].part.as[SingleQuery].clauses
        .should(matchPattern { case Seq(_: UnresolvedCall) => })
    }

    "inner query with imports and USE" in {
      val remote = asRemote(
        """WITH 1 AS a
          |CALL {
          |  USE foo
          |  WITH a AS a
          |  RETURN 1 AS y
          |}
          |RETURN 1 AS x
          |""".stripMargin,
        query => query.as[Fragment.Exec].input.as[Fragment.Apply].inner.as[Fragment.Exec],
        fabricContext = true
      )

      parse(remote.query).as[Query].part.as[SingleQuery].clauses
        .shouldEqual(Seq(
          with_(parameter("@@a", CTAny).as("a")),
          with_(varFor("a").as("a")),
          return_(literal(1).as("y"))
        ))
    }

    "union parts" in {
      val inst = instance(
        """WITH 1 AS a, 2 AS b
          |CALL {
          |  USE foo
          |  RETURN 3 AS c
          |    UNION
          |  USE foo
          |  WITH a
          |  RETURN a AS c
          |    UNION
          |  USE foo
          |  WITH a, b
          |  RETURN b AS c
          |    UNION
          |  USE bar
          |  WITH b
          |  RETURN b AS c
          |}
          |RETURN a, b, c
          |
          |""".stripMargin
      )
        .withForceFabricContext(true)

      val inner = inst.plan
        .query.as[Fragment.Exec]
        .input.as[Fragment.Apply]
        .inner.as[Fragment.Union]

      val part1 = inner.lhs.as[Fragment.Exec]
      val part2 = inner.rhs.as[Fragment.Exec]

      parse(inst.asRemote(part1).query)
        .shouldEqual(parse(
          """RETURN 3 AS c
            |  UNION
            |WITH $`@@a` AS a
            |WITH a
            |RETURN a AS c
            |  UNION
            |WITH $`@@a` AS a, $`@@b` AS b
            |WITH a, b
            |RETURN b AS c
            |""".stripMargin
        ))

      parse(inst.asRemote(part2).query)
        .shouldEqual(parse(
          """WITH $`@@b` AS b
            |WITH b
            |RETURN b AS c
            |""".stripMargin
        ))
    }

    "complicated nested query" ignore {
      val inst = instance(
        """WITH 1 AS a
          |CALL {
          |  WITH a
          |  WITH 2 AS b
          |  CALL {
          |    USE bar
          |    WITH b
          |    RETURN b AS c
          |  }
          |  CALL {
          |   RETURN 3 AS d
          |     UNION
          |   WITH b
          |   RETURN b AS d
          |     UNION
          |   USE baz
          |   WITH b
          |   RETURN b AS d
          |  }
          |  RETURN b, c, d
          |}
          |RETURN a, b, c, d
          |""".stripMargin
      )
        .withForceFabricContext(true)

      val execs = inst.plan.folded(Seq.empty[Fragment.Exec])(_ ++ _) {
        case exec: Fragment.Exec => Descend(Seq(exec))
      }

      val actual = execs.map(inst.asRemote).map(_.query)

      val expected = Seq(
        """WITH 1 AS a""",
        """WITH $`@@a` AS a
          |WITH a
          |WITH 2 AS b
          |""".stripMargin,
        """WITH $`@@b` AS b
          |WITH b
          |RETURN b AS c
          |""".stripMargin,
        """RETURN 3 AS d
          |  UNION
          |WITH $`@@b` AS b
          |WITH b
          |RETURN b AS d
          |""".stripMargin,
        """WITH $`@@b` AS b
          |WITH b
          |RETURN b AS d
          |""".stripMargin,
        """RETURN b, c, d
          |""".stripMargin
      )

      actual.shouldEqual(expected)

    }

    "union query with overloaded var names and aggregation should not fail" in {
      // This query crashed in Namespacer due to lost input positions
      val inst = instance(
        """
          |RETURN 'a' AS val, [] AS thisBreaks
          |UNION
          |CALL {
          |    WITH 'b' AS val RETURN val
          |    UNION
          |    WITH 'c' AS val RETURN val
          |}
          |WITH val, [v IN collect(val) WHERE v = 'd' | v] AS thisBreaks
          |RETURN val, thisBreaks
      """.stripMargin
      )

      // Assert that getting the plan does not fail
      inst.plan
    }

  }

  "asLocal: " - {

    "Variable with literal name" in {
      val inst = instance(
        """MATCH (n)
          |WITH n AS `true`
          |RETURN `true`"""
          .stripMargin
      )

      val exec = inst.plan.query.as[Fragment.Exec]

      val local = inst.asLocal(exec).query

      local.state.statement().shouldEqual(
        Query(
          None,
          singleQuery(
            match_(NodePattern(Some(varFor("n")), Seq.empty, None, None)(pos)),
            with_(varFor("n").as("true")),
            returnVars("true")
          )
        )(pos)
      )
      local.state.queryText should endWith("RETURN `true` AS `true`")
    }

    "Literal with variable with same name in scope" in {
      val inst = instance(
        """MATCH (n)
          |WITH n AS `true`
          |RETURN true"""
          .stripMargin
      )

      val exec = inst.plan.query.as[Fragment.Exec]

      val local = inst.asLocal(exec).query

      local.state.statement().shouldEqual(
        Query(
          None,
          singleQuery(
            match_(NodePattern(Some(varFor("n")), Seq.empty, None, None)(pos)),
            with_(varFor("n").as("true")),
            returnLit(true -> "true")
          )
        )(pos)
      )
      local.state.queryText should endWith("RETURN true AS `true`")
    }

    "FabricFrontEnd.parseAndPrepare anonymous names should not clash with FabricFrontEnd.checkAndFinalize anonymous names" in {
      // Parsing (which assign anonymous variables to PatternComprehension) is part of FabricFrontEnd.parseAndPrepare and
      // AddUniquenessPredicates is part of FabricFrontEnd.checkAndFinalize
      // These two phases both make use of the AnonymousVariableNameGenerator. This test is to show that they use the same AnonymousVariableNameGenerator
      // and no clashing names are generated.

      val inst = instance(
        """
          |MATCH (a)-[r]-(b)-[q*]-(c)
          |RETURN [(d)-[s]-(t) | d] AS p
          |""".stripMargin
      )
      val exec = inst.plan.query.as[Fragment.Exec]
      val statement = inst.asLocal(exec).query.state.statement()

      val whereAnons = statement.folder
        .treeFindByClass[Where].get.folder
        .findAllByClass[Variable]
        .map(_.name)
        .map(NameDeduplicator.removeGeneratedNamesAndParams)

      val pc = statement.folder.treeFindByClass[PatternComprehension].get
      val pAnons = Seq(pc.variableToCollectName, pc.collectionName)
        .map(NameDeduplicator.removeGeneratedNamesAndParams)

      pAnons should contain noElementsOf whereAnons
      // To protect from future changes, lets make sure we find anonymous variables in both cases
      whereAnons should not be empty
      pAnons should not be empty
    }
  }

  "Read/Write: " - {

    "read" in {
      plan("MATCH (x) RETURN *").queryType
        .shouldEqual(QueryType.Read)
    }
    "read + known read proc" in {
      plan("MATCH (x) CALL my.ns.read() YIELD a RETURN *").queryType
        .shouldEqual(QueryType.Read)
    }
    "read + known write proc" in {
      plan("MATCH (x) CALL my.ns.write() YIELD a RETURN *").queryType
        .shouldEqual(QueryType.Write)
    }
    "read + unknown proc" in {
      plan("MATCH (x) CALL my.ns.unknown() YIELD a RETURN *").queryType
        .shouldEqual(QueryType.ReadPlusUnresolved)
    }
    "write" in {
      plan("CREATE (x)").queryType
        .shouldEqual(QueryType.Write)
    }
    "write + known read proc" in {
      plan("CREATE (x) WITH * CALL my.ns.read() YIELD a RETURN *").queryType
        .shouldEqual(QueryType.Write)
    }
    "write + known write proc" in {
      plan("CREATE (x) WITH * CALL my.ns.write() YIELD a RETURN *").queryType
        .shouldEqual(QueryType.Write)
    }
    "write + unknown proc" in {
      plan("CREATE (x) WITH * CALL my.ns.unknown() YIELD a RETURN *").queryType
        .shouldEqual(QueryType.Write)
    }
    "per part" in {
      val pln = plan(
        """UNWIND [] AS x
          |CALL {
          |  USE g
          |  MATCH (n)
          |  CALL my.ns.unknown() YIELD a
          |  RETURN a AS a1
          |}
          |CALL {
          |  USE g
          |  MATCH (n)
          |  CALL my.ns.read() YIELD a
          |  RETURN a AS a2
          |}
          |CALL {
          |  USE g
          |  MATCH (n)
          |  CALL my.ns.write() YIELD a
          |  RETURN a AS a3
          |}
          |CALL {
          |  USE g
          |  CREATE (n) WITH *
          |  CALL my.ns.unknown() YIELD a
          |  RETURN a AS a4
          |}
          |CALL {
          |  USE g
          |  CREATE (n) WITH *
          |  CALL my.ns.read() YIELD a
          |  RETURN a AS a5
          |}
          |CALL {
          |  USE g
          |  CREATE (n) WITH *
          |  CALL my.ns.write() YIELD a
          |  RETURN a AS a6
          |}
          |RETURN *
          |""".stripMargin,
        fabricContext = true
      )

      val partsAsList = Stream
        .iterate(Option(pln.query)) {
          case Some(l: Fragment.Exec)  => Some(l.input)
          case Some(a: Fragment.Apply) => Some(a.input)
          case _                       => None
        }
        .takeWhile(_.isDefined)
        .collect { case Some(f) => f }
        .toList
        .reverse

      partsAsList
        .map(QueryType.local)
        .shouldEqual(Seq(
          QueryType.Read,
          QueryType.Read,
          QueryType.ReadPlusUnresolved,
          QueryType.Read,
          QueryType.Write,
          QueryType.Write,
          QueryType.Write,
          QueryType.Write,
          QueryType.Read
        ))

      pln.queryType
        .shouldEqual(QueryType.Write)
    }

  }

  "Cache:" - {

    "cache hit on equal input" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |WITH 3 AS z, y AS y
          |CALL {
          |  WITH 0 AS a
          |  RETURN 4 AS w
          |}
          |RETURN w, y
          |""".stripMargin

      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.instance(q, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(1)
      newPlanner.queryCache.getHits.shouldEqual(1)
    }

    "cache hit on equal input with query-obfuscation" in {
      val newPlanner = FabricPlanner(config, cypherConfigWithQueryObfuscation, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |WITH 3 AS z, y AS y
          |CALL {
          |  WITH 0 AS a
          |  RETURN 4 AS w
          |}
          |RETURN w, y
          |""".stripMargin

      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.instance(q, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(1)
      newPlanner.queryCache.getHits.shouldEqual(1)
    }

    "cache miss on different query" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q1 =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      val q2 =
        """WITH 1 AS x
          |RETURN x, 2 AS y
          |""".stripMargin

      newPlanner.instance(q1, params, defaultGraphName).plan
      newPlanner.instance(q2, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "cache miss on different default graph" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q, params, "foo").plan
      newPlanner.instance(q, params, "bar").plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "cache miss on options" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q1 =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      val q2 =
        """CYPHER debug=fabriclogplan
          |WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q1, params, defaultGraphName).plan
      newPlanner.instance(q2, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "cache miss on different param types" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of("a"))), defaultGraphName).plan
      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of(1))), defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "cache miss on new params" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of("a"))), defaultGraphName).plan
      newPlanner.instance(
        q,
        VirtualValues.map(Array("a", "b"), Array(Values.of("a"), Values.of(1))),
        defaultGraphName
      ).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "cache hit on different param values" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of("a"))), defaultGraphName).plan
      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of("b"))), defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(1)
      newPlanner.queryCache.getHits.shouldEqual(1)
    }

    "sensitive statements are not cached" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """CREATE USER foo SET PASSWORD 'secret'
          |""".stripMargin

      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.instance(q, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "sensitive statements are not cached using parameters" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """CREATE USER foo SET PASSWORD $p
          |""".stripMargin

      val secretParams = VirtualValues.map(Array("p"), Array(Values.stringValue("secret")))
      newPlanner.instance(q, secretParams, defaultGraphName).plan
      newPlanner.instance(q, secretParams, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }
    "cache miss on literal vs variable with same name" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q1 =
        """MATCH (n)
          |WITH n AS `true`
          |RETURN `true`
          |""".stripMargin
      val q2 =
        """MATCH (n)
          |WITH n AS `true`
          |RETURN true
          |""".stripMargin

      newPlanner.instance(q1, params, defaultGraphName).plan
      newPlanner.instance(q2, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
      newPlanner.queryCache.getHits.shouldEqual(0)
    }

    "cache clears a context" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.queryCache.contextSize(defaultGraphName).shouldEqual(1)
      newPlanner.queryCache.clearByContext(defaultGraphName).shouldEqual(1)
      newPlanner.queryCache.contextSize(defaultGraphName).shouldEqual(0)
    }

    "cache clears only the given context" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q1 =
        """USE foo
          |WITH 1 AS x
          |RETURN x
          |""".stripMargin
      val q2 =
        """USE bar
          |WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q1, params, "foo").plan
      newPlanner.instance(q2, params, "bar").plan
      newPlanner.queryCache.contextSize("foo").shouldEqual(1)
      newPlanner.queryCache.contextSize("bar").shouldEqual(1)
      newPlanner.queryCache.clearByContext("foo").shouldEqual(1)
      newPlanner.queryCache.contextSize("foo").shouldEqual(0)
      newPlanner.queryCache.contextSize("bar").shouldEqual(1)
    }

    "the cache hits before being cleared, and it misses after being cleared" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      // plan query (cold miss)
      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.queryCache.getHits.shouldEqual(0)
      newPlanner.queryCache.getMisses.shouldEqual(1)

      // replan query (hits)
      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.queryCache.getHits.shouldEqual(1)
      newPlanner.queryCache.getMisses.shouldEqual(1)

      // clear cache and rereplan query (cold miss again)
      newPlanner.queryCache.clearByContext(defaultGraphName)
      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.queryCache.getHits.shouldEqual(1)
      newPlanner.queryCache.getMisses.shouldEqual(2)
    }

    "clear an empty cache" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)
      newPlanner.queryCache.clearByContext(defaultGraphName).shouldEqual(0)
    }

    "clearing the cache returns the number of evictions" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val q =
        """WITH 1 AS x
          |RETURN x
          |""".stripMargin

      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.queryCache.clearByContext(defaultGraphName).shouldEqual(1)
      newPlanner.queryCache.clearByContext(defaultGraphName).shouldEqual(0)
    }

    "no cache hits for malformed options" in {
      case class ExpectedState(hits: Int, misses: Int, failure: Boolean)
      class CacheModel(var hits: Int = 0, var misses: Int = 0) {
        def hit(): ExpectedState = {
          hits += 1
          ExpectedState(hits, misses, failure = false)
        }

        def miss(): ExpectedState = {
          misses += 1
          ExpectedState(hits, misses, failure = false)
        }

        def fail(): ExpectedState = {
          ExpectedState(hits, misses, failure = true)
        }
      }

      val newPlanner = FabricPlanner(config, cypherConfig, monitors, cacheFactory, signatures)

      val model = new CacheModel()

      Seq(
        // PROFILE
        ("CYPHER runtime=slotted PROFILE RETURN 1", model.miss()),
        ("CYPHER PROFILE runtime=slotted RETURN 1", model.fail()),
        ("PROFILE CYPHER runtime=slotted RETURN 1", model.hit()),
        // EXPLAIN
        ("CYPHER runtime=slotted EXPLAIN RETURN 1", model.miss()),
        ("CYPHER EXPLAIN runtime=slotted RETURN 1", model.fail()),
        ("EXPLAIN CYPHER runtime=slotted RETURN 1", model.hit()),
        ("CYPHER runtime=slotted RETURN 1", model.hit()),
        // PROFILE with multiple options
        ("CYPHER runtime=slotted planner=dp PROFILE RETURN 1", model.miss()),
        ("CYPHER PROFILE planner=dp runtime=slotted RETURN 1", model.fail()),
        ("PROFILE CYPHER planner=dp runtime=slotted RETURN 1", model.hit()),
        // EXPLAIN with multiple options
        ("CYPHER runtime=slotted planner=dp EXPLAIN RETURN 1", model.miss()),
        ("CYPHER EXPLAIN planner=dp runtime=slotted RETURN 1", model.fail()),
        ("EXPLAIN CYPHER planner=dp runtime=slotted RETURN 1", model.hit()),
        // plain with multiple options
        ("CYPHER planner=dp runtime=slotted RETURN 1", model.hit()),
        ("CYPHER planner=dp runtime=slotted debug=toString RETURN 1", model.miss()),
        ("CYPHER planner=dp runtime=slotted debug=toString RETURN 1", model.hit())
      ).foreach { case (query, expectation) =>
        withClue(query) {
          val result = Try(newPlanner.instance(query, params, defaultGraphName).plan)
          result.isFailure.shouldEqual(expectation.failure)
          newPlanner.queryCache.getMisses.shouldEqual(expectation.misses)
          newPlanner.queryCache.getHits.shouldEqual(expectation.hits)
        }
      }

    }
  }

  "Options:" - {

    "allow EXPLAIN" in {
      val q =
        """EXPLAIN
          |RETURN 1 AS x
          |""".stripMargin

      plan(q)
        .check(_.executionType.shouldEqual(FabricPlan.EXPLAIN))
        .check(_.query.withoutLocalAndRemote.shouldEqual(
          init(defaultUse).exec(query(return_(literal(1).as("x"))), Seq("x"))
        ))
    }

    "allow single graph PROFILE" in {
      val q =
        """PROFILE
          |RETURN 1 AS x
          |""".stripMargin

      plan(q, params)
        .check(_.executionType.shouldEqual(FabricPlan.PROFILE))
        .check(_.query.withoutLocalAndRemote.shouldEqual(
          init(defaultUse).exec(query(return_(literal(1).as("x"))), Seq("x"))
        ))
    }

    "disallow multi graph PROFILE" in {
      val q =
        """PROFILE
          |UNWIND [0, 1] AS gid
          |CALL {
          | USE graph(gid)
          | RETURN 1
          |}
          |RETURN 1 AS x
          |""".stripMargin

      the[InvalidSemanticsException].thrownBy(plan(q, params, fabricContext = true))
        .check(_.getMessage.should(include("'PROFILE' not supported in Fabric context")))
    }

    "allow fabric debug options" in {
      val q =
        """CYPHER debug=fabriclogplan debug=fabriclogrecords
          |RETURN 1 AS x
          |""".stripMargin

      plan(q)
        .check(_.debugOptions.shouldEqual(DebugOptions(logPlan = true, logRecords = true)))
        .check(_.query.withoutLocalAndRemote.shouldEqual(
          init(defaultUse).exec(query(return_(literal(1).as("x"))), Seq("x"))
        ))
    }

    "passes periodic commit on to local parts" in {
      val inst = instance(
        """USING PERIODIC COMMIT 200
          |LOAD CSV FROM 'someurl' AS line
          |CREATE (n)
          |RETURN line
          |""".stripMargin
      )

      val exec = inst.plan.query.as[Fragment.Exec]

      val local = inst.asLocal(exec).query

      local.state.statement().shouldEqual(
        Query(
          Some(PeriodicCommitHint(Some(literalUnsignedInt(200)))(pos)),
          singleQuery(
            LoadCSV(withHeaders = false, literal("someurl"), varFor("line"), None)(pos),
            create(nodePat("n")),
            return_(varFor("line").as("line"))
          )
        )(pos)
      )

      local.options.isPeriodicCommit.shouldEqual(true)
    }

    "passes options on in remote and local parts" in {

      val inst = instance(
        """CYPHER 4.3
          |  planner=cost
          |  runtime=parallel
          |  updateStrategy=eager
          |  expressionEngine=compiled
          |  operatorEngine=interpreted
          |  interpretedPipesFallback=disabled
          |  replan=force
          |  connectComponentsPlanner=greedy
          |  debug=tostring
          |WITH 1 AS a
          |CALL {
          |  USE foo
          |  WITH a AS a
          |  RETURN 1 AS y
          |}
          |RETURN 1 AS x
          |""".stripMargin,
        fabricContext = true
      )

      val inner = inst.plan.query.as[Fragment.Exec].input.as[Fragment.Apply].inner.as[Fragment.Exec]
      val last = inst.plan.query.as[Fragment.Exec]

      val expectedInner = QueryOptions(
        offset = InputPosition.NONE,
        isPeriodicCommit = false,
        queryOptions = CypherQueryOptions(
          executionMode = CypherExecutionMode.default,
          version = CypherVersion.v4_3,
          planner = CypherPlannerOption.cost,
          runtime = CypherRuntimeOption.parallel,
          updateStrategy = CypherUpdateStrategy.eager,
          expressionEngine = CypherExpressionEngineOption.compiled,
          operatorEngine = CypherOperatorEngineOption.interpreted,
          interpretedPipesFallback = CypherInterpretedPipesFallbackOption.disabled,
          replan = CypherReplanOption.force,
          connectComponentsPlanner = CypherConnectComponentsPlannerOption.greedy,
          debugOptions = CypherDebugOptions(Set(CypherDebugOption.tostring))
        )
      )

      val expectedLast = QueryOptions.default.copy(
        queryOptions = QueryOptions.default.queryOptions.copy(
          runtime = CypherRuntimeOption.slotted,
          expressionEngine = CypherExpressionEngineOption.interpreted
        ),
        materializedEntitiesMode = true
      )

      preParse(inst.asRemote(inner).query).options.copy(offset = InputPosition.NONE)
        .shouldEqual(expectedInner)

      inst.asLocal(inner).query.options.copy(offset = InputPosition.NONE)
        .shouldEqual(expectedInner)

      inst.asLocal(last).query.options.copy(offset = InputPosition.NONE)
        .shouldEqual(expectedLast)
    }

    "default query options are not rendered" in {

      val inst = instance(
        """CYPHER 4.4
          |  interpretedPipesFallback=default
          |WITH 1 AS a
          |CALL {
          |  USE foo
          |  WITH a AS a
          |  RETURN 1 AS y
          |}
          |RETURN 1 AS x
          |""".stripMargin,
        fabricContext = true
      )

      val inner = inst.plan.query.as[Fragment.Exec].input.as[Fragment.Apply].inner.as[Fragment.Exec]
      val last = inst.plan.query.as[Fragment.Exec]

      val expectedInner = QueryOptions(
        offset = InputPosition.NONE,
        isPeriodicCommit = false,
        queryOptions = CypherQueryOptions.default
      )

      val expectedLast = QueryOptions.default.copy(
        queryOptions = QueryOptions.default.queryOptions.copy(
          runtime = CypherRuntimeOption.slotted,
          expressionEngine = CypherExpressionEngineOption.interpreted
        ),
        materializedEntitiesMode = true
      )

      val remote = inst.asRemote(inner).query
      remote.should(not(include("CYPHER")))

      preParse(remote).options.copy(offset = InputPosition.NONE)
        .shouldEqual(expectedInner)

      inst.asLocal(inner).query.options.copy(offset = InputPosition.NONE)
        .shouldEqual(expectedInner)

      inst.asLocal(last).query.options.copy(offset = InputPosition.NONE)
        .shouldEqual(expectedLast)
    }
  }

  "Descriptions:" - {

    "of stitched query" in {
      val desc = plan(
        """UNWIND [1, 2] AS x
          |CALL {
          |  RETURN 3 AS y
          |    UNION
          |  WITH 4 AS z
          |  RETURN z AS y
          |}
          |RETURN x, y
          |""".stripMargin
      ).query.description

      desc
        .check(_.getName.shouldEqual("Exec"))
        .check(_.getIdentifiers.shouldEqual(Set("x", "y").asJava))
        .check(_.getChildren.get(0)
          .check(_.getName.shouldEqual("Init")))
    }

    "of fabric query" in {
      val desc = plan(
        """UNWIND [1, 2] AS x
          |CALL {
          |  USE graph(x)
          |  RETURN 3 AS y
          |    UNION
          |  WITH 4 AS z
          |  RETURN z AS y
          |}
          |RETURN x, y
          |""".stripMargin,
        fabricContext = true
      ).query.description

      desc
        .check(_.getName.shouldEqual("Exec"))
        .check(_.getIdentifiers.shouldEqual(Set("x", "y").asJava))
        .check(_.getChildren.get(0)
          .check(_.getName.shouldEqual("Apply"))
          .check(_.getIdentifiers.shouldEqual(Set("x", "y").asJava))
          .check(_.getChildren.get(1)
            .check(_.getName.shouldEqual("Union"))
            .check(_.getChildren.get(0)
              .check(_.getName.shouldEqual("Exec"))
              .check(_.getIdentifiers.shouldEqual(Set("y").asJava)))
            .check(_.getChildren.get(1)
              .check(_.getName.shouldEqual("Exec"))
              .check(_.getIdentifiers.shouldEqual(Set("y").asJava))))
          .check(_.getChildren.get(0)
            .check(_.getName.shouldEqual("Exec"))
            .check(_.getIdentifiers.shouldEqual(Set("x").asJava))
            .check(_.getChildren.get(0)
              .check(_.getName.shouldEqual("Init")))))
    }
  }

  "Fragment stitching:" - {

    def defaultGraphQueries = Table(
      "query",
      s"""MATCH (n) RETURN n
         |""".stripMargin,
      s"""MATCH (n) RETURN n
         |  UNION
         |MATCH (n) RETURN n
         |""".stripMargin,
      s"""MATCH (n)
         |CALL {
         |  RETURN 1 AS a
         |}
         |RETURN n, a
         |""".stripMargin
    )

    def singleGraphQueries(graphName: String) =
      declared(graphName) ++
        declaredSubqueryInherited(graphName) ++
        declaredSubqueryInheritedSubqueryInherited(graphName)

    def singlePlusDefaultGraphQueries(graphName: String) =
      declaredUnionDefault(graphName)

    def defaultPlusSingleGraphQueries(graphName: String) =
      defaultUnionDeclared(graphName) ++
        defaultSubqueryDeclared(graphName)

    def declared(graphName: String) = Table(
      "query",
      s"""USE $graphName
         |MATCH (n) RETURN n
         |""".stripMargin,
      s"""USE $graphName
         |CREATE INDEX myIndex FOR (n:Label) ON (n.prop1, n.prop2)
         |""".stripMargin
    )

    def declaredSubqueryInherited(graphName: String) = Table(
      "query",
      s"""USE $graphName
         |MATCH (n)
         |CALL {
         |  RETURN 1 AS a
         |}
         |RETURN n, a
         |""".stripMargin
    )

    def declaredSubqueryInheritedSubqueryInherited(graphName: String) = Table(
      "query",
      s"""USE $graphName
         |MATCH (n)
         |CALL {
         |  CALL {
         |    RETURN 1 AS a
         |  }
         |  RETURN a AS b
         |}
         |RETURN n, b
         |""".stripMargin
    )

    def declaredUnionDefault(graphName: String) = Table(
      "query",
      s"""USE $graphName
         |MATCH (n) RETURN n
         |  UNION
         |MATCH (n) RETURN n
         |""".stripMargin
    )

    def defaultUnionDeclared(graphName: String) = Table(
      "query",
      s"""MATCH (n) RETURN n
         |  UNION
         |USE $graphName
         |MATCH (n) RETURN n
         |""".stripMargin
    )

    def defaultSubqueryDeclared(graphName: String) = Table(
      "query",
      s"""MATCH (n)
         |CALL {
         |  USE $graphName
         |  RETURN 1 AS a
         |}
         |RETURN n, a
         |""".stripMargin
    )

    def doubleGraphQueries(graphName1: String, graphName2: String) =
      declaredUnionDeclared(graphName1, graphName2) ++
        declaredSubqueryDeclared(graphName1, graphName2) ++
        declaredSubqueryDeclaredSubqueryInherited(graphName1, graphName2)

    def declaredUnionDeclared(graphName1: String, graphName2: String) = Table(
      "query",
      s"""USE $graphName1
         |MATCH (n) RETURN n
         |  UNION
         |USE $graphName2
         |MATCH (n) RETURN n
         |""".stripMargin
    )

    def declaredSubqueryDeclared(graphName1: String, graphName2: String) = Table(
      "query",
      s"""USE $graphName1
         |MATCH (n)
         |CALL {
         |  USE $graphName2
         |  RETURN 1 AS a
         |}
         |RETURN n, a
         |""".stripMargin
    )

    def declaredSubqueryDeclaredSubqueryInherited(graphName1: String, graphName2: String) = Table(
      "query",
      s"""USE $graphName1
         |MATCH (n)
         |CALL {
         |  USE $graphName2
         |  WITH 1 AS x
         |  CALL {
         |    RETURN 1 AS a
         |  }
         |  RETURN a AS b
         |}
         |RETURN n, b
         |""".stripMargin
    )

    def defaultSubqueryDeclaredUnionDeclared(graphName1: String, graphName2: String) = Table(
      "query",
      s"""CALL {
         |  USE $graphName1
         |  MATCH (n) RETURN n
         |    UNION
         |  USE $graphName2
         |  MATCH (n) RETURN n
         |}
         |RETURN n
         |""".stripMargin
    )

    def defaultSubqueryDeclaredUnionSubqueryDeclared(graphName1: String, graphName2: String) = Table(
      "query",
      s"""CALL {
         |  USE $graphName1
         |  MATCH (n) RETURN n
         |}
         |RETURN n
         |  UNION
         |CALL {
         |  USE $graphName2
         |  MATCH (n) RETURN n
         |}
         |RETURN n
         |""".stripMargin
    )

    def tripleGraphQueries(graphName1: String, graphName2: String, graphName3: String) =
      declaredUnionDeclaredUnionDeclared(graphName1, graphName2, graphName3) ++
        declaredSubqueryDeclaredSubqueryDeclared(graphName1, graphName2, graphName3)

    def declaredUnionDeclaredUnionDeclared(graphName1: String, graphName2: String, graphName3: String) = Table(
      "query",
      s"""USE $graphName1
         |MATCH (n) RETURN n
         |  UNION
         |USE $graphName2
         |MATCH (n) RETURN n
         |  UNION
         |USE $graphName3
         |MATCH (n) RETURN n
         |""".stripMargin
    )

    def declaredSubqueryDeclaredSubqueryDeclared(graphName1: String, graphName2: String, graphName3: String) = Table(
      "query",
      s"""USE $graphName1
         |MATCH (n)
         |CALL {
         |  USE $graphName2
         |  WITH 1 AS a
         |  CALL {
         |    USE $graphName3
         |    RETURN 1 AS b
         |  }
         |  RETURN a
         |}
         |RETURN n, a
         |""".stripMargin
    )

    def planAndStitch(sessionGraphName: String, fabricName: String, query: String, params: MapValue = params) = {
      val planner =
        FabricPlanner(makeConfig(fabricName), cypherConfig, monitors, cacheFactory, signatures)
          .instance(query, params, sessionGraphName)
      Try(planner.plan)
    }

    val fabricName = "fabric"
    val sessionGraphName = "session"

    "stitches single-graph queries" in {

      forAll(defaultGraphQueries) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(singleGraphQueries(graphName = "foo")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(singleGraphQueries(graphName = "foo.bar")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }
    }

    "stitches multi-graph queries when graph is the same" in {

      forAll(singlePlusDefaultGraphQueries(graphName = sessionGraphName)) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(defaultPlusSingleGraphQueries(graphName = sessionGraphName)) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(doubleGraphQueries(graphName1 = "foo", graphName2 = "foo")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(doubleGraphQueries(graphName1 = "foo.bar", graphName2 = "foo.bar")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(doubleGraphQueries(graphName1 = "foo", graphName2 = "foo")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }

      forAll(tripleGraphQueries(graphName1 = "foo.bar", graphName2 = "foo.bar", graphName3 = "foo.bar")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(beFullyStitched)
      }
    }

    "disallows dynamic USE outside fabric" in {

      forAll(singleGraphQueries(graphName = "f(1)")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(singleGraphQueries(graphName = "$p")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(singlePlusDefaultGraphQueries(graphName = "f(1)")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(defaultPlusSingleGraphQueries(graphName = "f(1)")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(doubleGraphQueries(graphName1 = "foo", graphName2 = "f(1)")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(tripleGraphQueries(graphName1 = "foo.bar", graphName2 = "f(1)", graphName3 = "foo.bar")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }
    }

    "disallows multi-graph outside fabric" in {

      forAll(singlePlusDefaultGraphQueries(graphName = "foo")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(defaultPlusSingleGraphQueries(graphName = "foo")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(doubleGraphQueries(graphName1 = "foo", graphName2 = "bar")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }

      forAll(tripleGraphQueries(graphName1 = "foo.bar", graphName2 = "foo", graphName3 = "foo.bar")) { query =>
        planAndStitch(sessionGraphName, fabricName, query)
          .should(matchPattern { case Failure(_) => })
      }
    }

    "in fabric context" - {

      "stitches single-graph queries" in {

        forAll(defaultGraphQueries) { query =>
          planAndStitch(fabricName, fabricName, query)
            .should(beFullyStitched)
        }

        forAll(singleGraphQueries(graphName = "foo")) { query =>
          planAndStitch(fabricName, fabricName, query)
            .should(beFullyStitched)
        }

        forAll(declaredUnionDeclared(graphName1 = fabricName, graphName2 = fabricName)) { query =>
          planAndStitch(sessionGraphName, fabricName, query)
            .should(beFullyStitched)
        }

        forAll(declaredUnionDeclaredUnionDeclared(
          graphName1 = fabricName,
          graphName2 = fabricName,
          graphName3 = fabricName
        )) { query =>
          planAndStitch(sessionGraphName, fabricName, query)
            .should(beFullyStitched)
        }

      }

      "leaves multi-graph queries un-stitched" - {

        "implicit" in {

          forAll(defaultSubqueryDeclared(graphName = "foo")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(defaultSubqueryDeclaredUnionDeclared(graphName1 = "foo", graphName2 = "bar")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(declaredUnionDeclared(graphName1 = "foo", graphName2 = "bar")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(declaredUnionDeclaredUnionDeclared(graphName1 = "foo", graphName2 = "bar", graphName3 = "baz")) {
            query =>
              planAndStitch(fabricName, fabricName, query)
                .should(matchPattern { case Success(_) => })
          }

          forAll(defaultSubqueryDeclaredUnionSubqueryDeclared(graphName1 = "foo", graphName2 = "bar")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

        }

        "explicit" in {

          forAll(declaredSubqueryDeclared(graphName1 = fabricName, graphName2 = "foo")) { query =>
            planAndStitch(sessionGraphName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryInherited(graphName1 = fabricName, graphName2 = "foo")) { query =>
            planAndStitch(sessionGraphName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }
        }

        "implicit plus dynamic" in {

          forAll(defaultSubqueryDeclared(graphName = "f(1)")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(defaultSubqueryDeclaredUnionDeclared(graphName1 = "f(1)", graphName2 = "bar.f(2)")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(singleGraphQueries(graphName = "f(1)")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(declaredUnionDeclared(graphName1 = "f(1)", graphName2 = "g(2)")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(declaredUnionDeclaredUnionDeclared(graphName1 = "f(1)", graphName2 = "g(2)", graphName3 = "h(3)")) {
            query =>
              planAndStitch(fabricName, fabricName, query)
                .should(matchPattern { case Success(_) => })
          }
        }

        "explicit plus dynamic" in {

          forAll(declaredSubqueryDeclared(graphName1 = fabricName, graphName2 = "f(1)")) { query =>
            planAndStitch(sessionGraphName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryInherited(graphName1 = fabricName, graphName2 = "f(1)")) { query =>
            planAndStitch(sessionGraphName, fabricName, query)
              .should(matchPattern { case Success(_) => })
          }

        }

        "fails on nested USE" in {

          forAll(declaredSubqueryDeclared(graphName1 = "foo", graphName2 = "bar")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclared(graphName1 = "f(1)", graphName2 = "g(2)")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryInherited(graphName1 = "foo", graphName2 = "bar")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryInherited(graphName1 = "f(1)", graphName2 = "g(2)")) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryDeclared(
            graphName1 = fabricName,
            graphName2 = "foo.bar(1)",
            graphName3 = "foo.baz(2)"
          )) { query =>
            planAndStitch(sessionGraphName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryDeclared(graphName1 = "foo", graphName2 = "bar", graphName3 = "baz")) {
            query =>
              planAndStitch(fabricName, fabricName, query)
                .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryDeclared(
            graphName1 = fabricName,
            graphName2 = "foo.bar",
            graphName3 = "foo.baz"
          )) { query =>
            planAndStitch(sessionGraphName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

          forAll(declaredSubqueryDeclaredSubqueryDeclared(
            graphName1 = "f(1)",
            graphName2 = "g(2)",
            graphName3 = "h(3)"
          )) { query =>
            planAndStitch(fabricName, fabricName, query)
              .should(matchPattern { case Failure(_) => })
          }

        }
      }
    }
  }

  "FrontEnd setup:" - {

    "compilation tracing" in {
      object eventListener extends TimingCompilationTracer.EventListener {
        var queries: Seq[String] = Seq()
        var events: Seq[TimingCompilationTracer.QueryEvent] = Seq()
        override def startQueryCompilation(query: String): Unit = queries = queries :+ query
        override def queryCompiled(event: TimingCompilationTracer.QueryEvent): Unit = events = events :+ event
        def use(func: => Unit): Unit = {
          try {
            monitors.addMonitorListener(this)
            func
          } finally {
            monitors.removeMonitorListener(this)
            queries = Seq()
            events = Seq()
          }
        }
      }

      eventListener.use {

        plan("RETURN 1")

        eventListener.queries
          .should(contain("RETURN 1"))

        eventListener.events.map(_.query())
          .should(contain("RETURN 1"))
      }

    }

    "parameter types" in {

      val inst = instance(
        "RETURN $p AS p",
        VirtualValues.map(Array("p"), Array(Values.of(1)))
      )

      val local = inst.asLocal(inst.plan.query.as[Fragment.Exec])

      local.query.state.statement()
        .shouldEqual(
          query(return_(parameter("p", ct.int).as("p")))
        )

    }
  }

  "Misc:" - {
    "allow length() on lists in CYPHER 3.5 mode" in {
      val q =
        """EXPLAIN
          |CYPHER 3.5
          |WITH range(1, 10) AS list
          |RETURN length(list) AS result
          |""".stripMargin

      val res = plan(q).query.withoutLocalAndRemote

      val expectedResult = init(defaultUse).exec(
        query = query(
          with_(function("range", literal(1), literal(10)) as "list"),
          return_(length3_5(varFor("list")) as "result")
        ),
        outputColumns = Seq("result")
      )

      res shouldBe expectedResult
    }
  }

  implicit class FabricCacheOps(cache: FabricQueryCache) {

    def contextSize(contextName: String): Int =
      cache.getInnerCopy.collect { case ((_, _, `contextName`), _) => }.size
  }

  implicit class CheckSyntax[A](a: A) {

    def check(f: A => Any): A = {
      f(a)
      a
    }
  }

  implicit class FullyParsedQueryHelp(q: FullyParsedQuery) {

    def asSingleQuery: SingleQuery =
      q.state.statement().as[Query].part.as[SingleQuery]
  }

  val beFullyStitched: Matcher[Try[FabricPlan]] = Matcher[Try[FabricPlan]] {
    case Success(value) =>
      value.query match {
        case frag @ Fragment.Exec(_: Fragment.Init, _, _, _, _, _) =>
          MatchResult(matches = true, s"Expectation failed, got: $frag", s"Expectation failed, got: $frag")

        case frag => MatchResult(
            matches = false,
            s"Expected fully stitched query, but got: $frag",
            s"Expectation failed, got: $frag"
          )
      }
    case Failure(exception) => MatchResult(
        matches = false,
        s"Expected fully stitched query, but got exception: ${exception.getMessage}",
        s"Expectation failed, got exception: ${exception.getMessage}"
      )
  }
}
