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
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.Outcome
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.checker.CompositionRestriction.NoCountOrExistsSubqueryBody
import org.neo4j.cypher.internal.frontend.scoping.checker.CompositionRestriction.NoLocalCallableBody

import scala.util.Random

object SurroundGivenQueriesWithLocalCallablesDefinition extends LocalCallableGenHelpers {

  def forAll(queries: Seq[TestQuery])(implicit rand: Random): Iterable[TestQuery] = {
    val indexes = queries.indices
    for {
      testQueryIndex <- indexes
      testQuery = queries(testQueryIndex)
      filteredQueries = queries.filter(_.compositionRestriction != NoLocalCallableBody)
      seed = calcSeed(testQueryIndex)
    } yield {
      _surround(testQuery, seed, filteredQueries)
    }
  }

  def sample(queries: Seq[TestQuery], numSamples: Int): Iterable[TestQuery] = {
    for {
      sampleNum <- 1 to numSamples
      seed = calcSeed(sampleNum)
      sampleRand = new Random(seed)
      filteredQueries = queries.filter(_.compositionRestriction != NoLocalCallableBody)
      testQueryIndex = pickOne(filteredQueries.indices)(sampleRand)
      testQuery = filteredQueries(testQueryIndex)
    } yield {
      _surround(testQuery, seed, filteredQueries)(sampleRand)
    }
  }

  def sample(testQueries: Seq[TestQuery], poolQueries: Seq[TestQuery], numSamples: Int): Iterable[TestQuery] = {
    for {
      sampleNum <- 1 to numSamples
      seed = calcSeed(sampleNum)
      sampleRand = new Random(seed)
      filteredTestQueries = testQueries.filter(_.compositionRestriction != NoLocalCallableBody)
      filteredPoolQueries = poolQueries.filter(_.compositionRestriction != NoLocalCallableBody)
      testQueryIndex = pickOne(filteredTestQueries.indices)(sampleRand)
      testQuery = filteredTestQueries(testQueryIndex)
    } yield {
      _surround(testQuery, seed, filteredPoolQueries)(sampleRand)
    }
  }

  def sampleBySeed(queries: Seq[TestQuery], seed: Int): TestQuery = {
    val sampleRand = new Random(seed)
    val filteredQueries = queries.filter(_.compositionRestriction != NoLocalCallableBody)
    val testQueryIndex = pickOne(filteredQueries.indices)(sampleRand)
    val testQuery = filteredQueries(testQueryIndex)
    _surround(testQuery, seed, filteredQueries)(sampleRand)
  }

  private def calcSeed(sampleNum: Int): Int = (sampleNum + 1) * 13

  private val baselineTestQuery = Seq(
    TestQuery(
      """MATCH (n)
        |RETURN n""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """MATCH (n:A)-->(m)
        |RETURN n, m
        |UNION
        |MATCH (n:B)<--(m)
        |RETURN n, m""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """UNWIND [1,2,3] AS x
        |CREATE (:A {id: x})
        |FINISH""".stripMargin,
      Passes,
      Seq()
    )
  )

  private def _surround(query: TestQuery, seed: Int, queries: Seq[TestQuery])(implicit rand: Random): TestQuery = {
    implicit val names: DistinctNames = new DistinctNames
    val candidateQueries = (baselineTestQuery ++ queries).filter(tq => tq != query && tq.outcome == Passes)
    val maxNestingLevel = pickOne(1 to 3)
    val surroundTestQuery = _nest(query, candidateQueries, maxNestingLevel)
    surroundTestQuery.copy(
      cypher =
        s"""/* seed: $seed */
           |${surroundTestQuery.cypher}""".stripMargin
    )
  }

  private def _nest(
    query: TestQuery,
    candidateQueries: Seq[TestQuery],
    maxNestingLevel: Int
  )(implicit names: DistinctNames, rand: Random): TestQuery = {
    val otherQueries = _otherQueries(query, candidateQueries)
    if (maxNestingLevel <= 1) {
      _flatQueryForGivenQueries(otherQueries :+ query, query.outcome)
    } else {
      val outcome = query.outcome
      val (queriesToNest, queriesNotToNest) = {
        val queryToNest = Math.round((otherQueries.size + 1) * 0.5f)
        pickDistinct(rand.shuffle(otherQueries :+ query), queryToNest)
      }
      val queries =
        queriesToNest.map(q => _nest(q, candidateQueries, maxNestingLevel - 1)) ++
          queriesNotToNest.map(q => _nest(q, candidateQueries, maxNestingLevel - 2))
      _flatQueryForGivenQueries(queries, outcome)
    }
  }

  private def _otherQueries(
    query: TestQuery,
    candidateQueries: Seq[TestQuery]
  )(implicit rand: Random): Seq[TestQuery] = {
    val trueCandidateQueries =
      candidateQueries.filter(_.returnColumns != query.returnColumns).groupBy(_.returnColumns).map {
        case (_, candidates) => pickOne(candidates)
      }.toSeq
    val numOtherQueries = Math.min(pickOne(Seq(0, 1, 3, 5)), trueCandidateQueries.size)
    val (otherQueries, _) = pickDistinct(trueCandidateQueries, numOtherQueries)
    otherQueries
  }

  private def _flatQueryForGivenQueries(
    queries: Seq[TestQuery],
    outcome: Outcome
  )(implicit names: DistinctNames, rand: Random): TestQuery = {
    val definitions = queries.map(tq => _definition(tq))
    val defs = rand.shuffle(definitions).map(_.cypher).mkString(System.lineSeparator())
    val definitionsInCallOrder = rand.shuffle(definitions)
    val yields = definitionsInCallOrder.foldLeft(Seq.empty[Seq[String]]) {
      case (before, definition) if definition.kind == Procedure =>
        before :+ definition.returnColumns.filter(c => !before.flatten.contains(c))
      case (before, definition) if definition.kind == Function =>
        before :+ Seq(names.next())
      case (before, _) => before
    }
    val definitionsInCallOrderWithYield = definitionsInCallOrder.zip(yields)
    val calls = definitionsInCallOrderWithYield.map {
      case (definition, yieldCols) => _call(definition, yieldCols)
    }.mkString(System.lineSeparator())
    val allYieldFromCallables = yields.flatten
    val returnVar = pickOne(allYieldFromCallables :+ "123")
    val returnCol = names.next()
    val query = s"""$defs
                   |$calls
                   |RETURN $returnVar AS $returnCol""".stripMargin
    TestQuery(query, outcome, Seq(returnCol))
  }

  private def _call(definition: Definition, yieldCols: Seq[String]): String = definition match {
    case Definition(kind, name, numParameters, _, _) =>
      val args = (0 until numParameters).map(i => s"$i").mkString(", ")
      val invoc = s"$name($args)".stripMargin
      kind match {
        case Procedure if definition.returnColumns.nonEmpty && yieldCols.isEmpty =>
          s"// no call of ${definition.name}"
        case Procedure =>
          val yld =
            if (yieldCols.isEmpty) ""
            else s"YIELD ${yieldCols.mkString(", ")}"
          s"CALL $invoc $yld"
        case Function =>
          val yld = yieldCols.head
          s"LET $yld = $invoc"
      }
  }

  case class Definition(kind: Kind, name: String, numParameters: Int, cypher: String, returnColumns: Seq[String])

  private def _definition(inner: TestQuery)(implicit names: DistinctNames, rand: Random): Definition = {
    val name = names.next(5)
    val numParameters = pickOne(Seq(0, 1, 2, 3, 5))
    val parameters = variableNames(numParameters).map(p => s"_param_$p")
    val parametersDef = parameters.mkString(", ")
    val kind = pickOne(Seq(Procedure, Function))
    val returnColumns = kind match {
      case Procedure => inner.returnColumns
      case Function  => Seq.empty
    }
    val bodies = kind match {
      case Procedure => Seq(
          Query(inner.cypher)
        )
      case Function =>
        (if (inner.compositionRestriction == NoCountOrExistsSubqueryBody) Seq.empty
         else {
           Seq(
             Expression(
               s"""EXISTS {
                  |${indent(inner.cypher, "  ")}
                  |}""".stripMargin,
               "foo"
             ),
             Expression(
               s"""COUNT {
                  |${indent(inner.cypher, "  ")}
                  |}""".stripMargin,
               "foo"
             )
           )
         }) ++ Seq(
          Expression(
            s"""EXISTS {
               |  CALL () {
               |${indent(inner.cypher, "    ")}
               |  }
               |}""".stripMargin,
            "foo"
          ),
          Expression(
            s"""COUNT {
               |  CALL () {
               |${indent(inner.cypher, "    ")}
               |  }
               |}""".stripMargin,
            "foo"
          )
        )
    }
    val body = pickOne(bodies)
    Definition(
      kind,
      name,
      numParameters,
      s"""DEFINE ${kind.keyword} $name($parametersDef) ${body.renderFor(kind)}""".stripMargin,
      returnColumns
    )
  }

  class DistinctNames {
    var i: Int = 0

    def next(maxNumComponents: Int = 1)(implicit rand: Random): String = {
      i = i + rand.nextInt(10)
      name(maxNumComponents) + i
    }

    private def name(maxNumComponents: Int)(implicit rand: Random): String = {
      val chars = ('a' to 'z') ++ ('A' to 'Z')
      val numComponents = pickOne(1 to maxNumComponents)
      (1 to numComponents).map {
        _ =>
          val componentLength = pickOne(Seq(2, 3, 4, 5, 6, 15))
          (1 to componentLength).map {
            _ => pickOne(chars).toString
          }.mkString
      }.mkString(".")
    }
  }
}
