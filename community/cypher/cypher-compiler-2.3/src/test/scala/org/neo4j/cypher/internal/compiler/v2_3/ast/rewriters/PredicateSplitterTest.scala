/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

object PredicateSplitterTest extends AstConstructionTestSupport {
  val RETURN_ALL = Return(distinct = false, ReturnItems(includeExisting = true, Seq.empty)_, None, None, None)_

  private def query(clauses: Clause*): Statement = Query(None, SingleQuery(clauses)_)_
  private def ri(name: String): ReturnItem = AliasedReturnItem(ident(name), ident(name))_
  private def items(name: String*): ReturnItems = ReturnItems(includeExisting = false, name.toSet.toSeq.map(ri))_
  private def pathPred(num: Int): Expression = GreaterThan(ContainerIndex(ident("p"), SignedDecimalIntegerLiteral(num.toString)_)_, SignedDecimalIntegerLiteral("1")_)_
}

class PredicateSplitterTest extends CypherFunSuite with AstConstructionTestSupport {

  import PredicateSplitterTest._

  test("Does not change MATCH without named paths") {
    val MATCH: Clause = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false) _)))_, Seq.empty, Some(Where(True()_)_))_
    val statement: Statement = query(MATCH, RETURN_ALL)
    val lookup = (clause: Clause) => clause match {
      case MATCH => Set(ident("b"))
      case _ => Set.empty[Identifier]
    }

    val splitter = PredicateSplitter(lookup, statement)
    val rewritten = splitter.statementRewriter(statement)

    rewritten should equal(statement)
  }

  test("Moves one predicate over named paths to newly introduced WITH") {
    val pred: Expression = pathPred(0)
    val MATCH: Clause = Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, Some(Where(pred)_))_
    val statement: Statement = query(MATCH, RETURN_ALL)
    val lookup = (clause: Clause) => clause match {
      case MATCH => Set(ident("a"), ident("b"))
      case _ => Set.empty[Identifier]
    }

    val splitter = PredicateSplitter(lookup, statement)
    val rewritten = splitter.statementRewriter(statement)

    val EXPECTED_MATCH: Clause = Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, None)_
    val EXPECTED_WITH: Clause = With(distinct = false, items("a", "b", "p"), None, None, None, Some(Where(pred)_))_
    val expectation = query(EXPECTED_MATCH, EXPECTED_WITH, RETURN_ALL)

    rewritten should equal(expectation)
  }

  test("Moves two predicates over named paths to newly introduced WITH") {
    val pred: Expression = And(pathPred(0), pathPred(1))_
    val MATCH: Clause = Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, Some(Where(pred)_))_
    val statement: Statement = query(MATCH, RETURN_ALL)
    val lookup = (clause: Clause) => clause match {
      case MATCH => Set(ident("a"), ident("b"))
      case _ => Set.empty[Identifier]
    }

    val splitter = PredicateSplitter(lookup, statement)
    val rewritten = splitter.statementRewriter(statement)

    val EXPECTED_MATCH: Clause = Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, None)_
    val EXPECTED_WITH: Clause = With(distinct = false, items("a", "b", "p"), None, None, None, Some(Where(pred)_))_
    val expectation = query(EXPECTED_MATCH, EXPECTED_WITH, RETURN_ALL)

    rewritten should equal(expectation)
  }

  test("Moves predicate over named paths to newly introduced WITH but keeps other predicate on MATCH") {
    val pred1: Expression = Equals(ident("a"), ident("b"))_
    val pred2: Expression = pathPred(1)
    val pred: Expression = Ands(Set(pred1, pred2))_
    val MATCH: Clause = Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, Some(Where(pred)_))_
    val statement: Statement = query(MATCH, RETURN_ALL)
    val lookup = (clause: Clause) => clause match {
      case MATCH => Set(ident("a"), ident("b"))
      case _ => Set.empty[Identifier]
    }

    val splitter = PredicateSplitter(lookup, statement)
    val rewritten = splitter.statementRewriter(statement)

    val EXPECTED_MATCH: Clause = Match(optional = false, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, Some(Where(pred1)_))_
    val EXPECTED_WITH: Clause = With(distinct = false, items("a", "b", "p"), None, None, None, Some(Where(pred2)_))_
    val expectation = query(EXPECTED_MATCH, EXPECTED_WITH, RETURN_ALL)

    rewritten should equal(expectation)
  }

  test("Does not move predicate over named paths to newly introduced WITH from OPTIONAL MATCH") {
    val pred: Expression = pathPred(0)
    val OPTIONAL_MATCH: Clause = Match(optional = true, Pattern(Seq(NamedPatternPart(ident("p"), EveryPath(NodePattern(Some(ident("a")), Seq.empty, None, naked = false)_))_))_, Seq.empty, Some(Where(pred)_))_
    val statement: Statement = query(OPTIONAL_MATCH, RETURN_ALL)
    val lookup = (clause: Clause) => clause match {
      case OPTIONAL_MATCH => Set(ident("a"), ident("b"))
      case _ => Set.empty[Identifier]
    }

    val splitter = PredicateSplitter(lookup, statement)
    val rewritten = splitter.statementRewriter(statement)

    rewritten should equal(statement)
  }
}
