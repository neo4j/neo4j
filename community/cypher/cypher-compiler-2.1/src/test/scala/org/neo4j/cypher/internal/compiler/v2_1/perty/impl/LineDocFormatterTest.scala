/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.perty.impl

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.perty.{PrintText, Doc}

class LineDocFormatterTest extends CypherFunSuite {

  import Doc._

  test("format ConsDoc") {
    LineDocFormatter(cons("a", cons("b"))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NilDoc") {
    LineDocFormatter(nil) should equal(Seq.empty)
  }

  test("format TextDoc") {
    LineDocFormatter("text") should equal(Seq(PrintText("text")))
  }

  test("format BreakDoc") {
    LineDocFormatter(break) should equal(Seq(PrintText(" ")))
  }

  test("format BreakWith") {
    LineDocFormatter(breakWith("-")) should equal(Seq(PrintText("-")))
  }

  test("format GroupDoc") {
    LineDocFormatter(group(cons("a", "b"))) should equal(Seq(PrintText("a"), PrintText("b")))
  }

  test("format NestDoc") {
    LineDocFormatter(nest(10, nil)) should equal(Seq.empty)
  }

  test("format PageDoc") {
    LineDocFormatter(page(nil)) should equal(Seq.empty)
  }
}
