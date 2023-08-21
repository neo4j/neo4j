/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.util.symbols.CTInteger

class TestPlanBuilder extends SimpleLogicalPlanBuilder(TestResolver)

object TestResolver extends SimpleResolver(procedures =
      Set(
        ProcedureSignature(
          QualifiedName(Seq("test"), "proc1"),
          IndexedSeq(),
          None,
          None,
          ProcedureReadOnlyAccess,
          id = 0
        ),
        ProcedureSignature(
          QualifiedName(Seq("test"), "proc2"),
          IndexedSeq(FieldSignature("in1", CTInteger)),
          Some(IndexedSeq(FieldSignature("foo", CTInteger))),
          None,
          ProcedureReadOnlyAccess,
          id = 0
        )
      )
    )
