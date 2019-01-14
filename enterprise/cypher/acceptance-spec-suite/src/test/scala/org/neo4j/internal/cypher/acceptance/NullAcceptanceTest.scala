/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
