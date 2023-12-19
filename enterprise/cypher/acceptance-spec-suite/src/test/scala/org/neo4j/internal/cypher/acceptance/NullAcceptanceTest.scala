/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class NullAcceptanceTest extends ExecutionEngineFunSuite {

  val anyNull: AnyRef = null.asInstanceOf[AnyRef]
  val expressions = Seq(
    "round(null)",
    "floor(null)",
    "ceil(null)",
    "abs(null)",
    "acos(null)",
    "asin(null)",
    "atan(null)",
    "cos(null)",
    "cot(null)",
    "exp(null)",
    "log(null)",
    "log10(null)",
    "sin(null)",
    "tan(null)",
    "haversin(null)",
    "sqrt(null)",
    "sign(null)",
    "radians(null)",
    "atan2(null, 0.3)",
    "atan2(0.3, null)",
    "null in [1,2,3]",
    "2 in null",
    "null in null",
    "ANY(x in NULL WHERE x = 42)"
  )

  expressions.foreach { expression =>
    test(expression) {
      executeScalar[Any]("RETURN " + expression) should equal(anyNull)
    }
  }
}
