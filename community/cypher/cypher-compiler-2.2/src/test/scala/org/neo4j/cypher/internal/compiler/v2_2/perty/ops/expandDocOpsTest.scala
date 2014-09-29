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
package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.perty.Extractor

class expandDocOpsTest extends CypherFunSuite {

  import Extractor._
  import NewPretty._

  test("passes through plain doc ops") {
    import org.neo4j.cypher.internal.compiler.v2_2.perty.ops.NewPretty._

    val doc = NewPretty("x" :/: "y")
    val result = expandDocOps(Extractor.empty).apply(doc)

    result should equal(doc)
  }

  test("replaces content in doc ops") {
    val doc = NewPretty(pretty(1) :/: "y")
    val result = expandDocOps[Int](pick {
      case (a: Int) => NewPretty("1")
    }).apply(doc)

    result should equal(NewPretty("1" :/: "y"))
  }
}
