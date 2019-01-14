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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.runtime.planDescription.{NoChildren, PlanDescriptionImpl, renderAsTreeTable}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.test_helpers.{CypherFunSuite, WindowsStringSafe}

class ExecutionPlanDescriptionTest extends CypherFunSuite {
  implicit val windowsSafe = WindowsStringSafe

  test("use variable name instead of ReferenceFromSlot") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Expression(ReferenceFromSlot(42, "  id@23")),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(Id.INVALID_ID, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderAsTreeTable(plan)
    details should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | id    |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

}
