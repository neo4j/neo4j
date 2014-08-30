/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import org.neo4j.cypher.internal.commons.CypherFunSuite

class ExtractionFailureHandlerTest extends CypherFunSuite {
  type TestDrill = Drill[Any, Any, String]
  type TestDigger = Digger[Any, Any, String]

  test("propagateExtractionFailure propagates extraction failures") {
    case class X(v: Any, opt: Option[X])
    val drill1: TestDrill = mkDrill[Any, Any, String](propagateExtractionFailure()) {
      case Some(X(v, opt)) => inner => s"X(${inner(v)}, ${inner(opt)})"
      case None => inner            => "None"
    }
    val drill2: TestDrill = mkDrill(propagateExtractionFailure[Any, String]()) {
      case x: Int => inner => x.toString
    }

    val extractor = FunDigger(drill1, drill2).asExtractor

    extractor(Some(X(1, Some(X(2, None))))) should equal(Some("X(1, X(2, None))"))
    extractor(Some(X(1, Some(X(true, None))))) should equal(None)
  }

  test("replaceExtractionFailure replaces extraction failures with a default replacement") {
    case class X(v: Any, opt: Option[X])
    val drill1: TestDrill = mkDrill[Any, Any, String](replaceExtractionFailure("!!!")) {
      case Some(X(v, opt)) => inner => s"X(${inner(v)}, ${inner(opt)})"
      case None => inner            => "None"
    }
    val drill2: TestDrill = mkDrill(propagateExtractionFailure[Any, String]()) {
      case x: Int => inner => x.toString
    }

    val extractor = FunDigger(drill1, drill2).asExtractor

    extractor(Some(X(1, Some(X(2, None))))) should equal(Some("X(1, X(2, None))"))
    extractor(Some(X(1, Some(X(true, None))))) should equal(Some("X(1, X(!!!, None))"))
  }

  test("catchErrorsDuringExtraction prints extraction failures") {
    case class X(v: Any, opt: Option[X])
    val drill1: TestDrill = mkDrill[Any, Any, String](mapExtractionFailure[Exception, Any, String](e => Some(e.getMessage))) {
      case Some(X(v, opt)) => inner => s"X(${inner(v)}, ${inner(opt)})"
      case None => inner            => "None"
    }
    val drill2: TestDrill = mkDrill(propagateExtractionFailure[Any, String]()) {
      case x: Int => inner => x.toString
    }

    val extractor = FunDigger(drill1, drill2).asExtractor

    extractor(Some(X(1, Some(X(2, None))))) should equal(Some("X(1, X(2, None))"))
    extractor(Some(X(1, Some(X(true, None))))) should equal(Some("X(1, None.get)"))
  }
}
