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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.ast.FunctionPrivilegeQualifier
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedurePrivilegeQualifier
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.internal.kernel.api.security.FunctionSegment
import org.neo4j.internal.kernel.api.security.LabelSegment
import org.neo4j.internal.kernel.api.security.ProcedureSegment
import org.neo4j.internal.kernel.api.security.RelTypeSegment
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.internal.kernel.api.security.UserSegment

object QualifierMapper {

  def asKernelQualifier(qualifier: PrivilegeQualifier): Segment = qualifier match {
    case _: ProcedurePrivilegeQualifier => ProcedureSegment.ALL
    case _: FunctionPrivilegeQualifier  => FunctionSegment.ALL
    case _: LabelQualifier              => LabelSegment.ALL
    case _: LabelAllQualifier           => LabelSegment.ALL
    case _: RelationshipQualifier       => RelTypeSegment.ALL
    case _: RelationshipAllQualifier    => RelTypeSegment.ALL
    case _: UserQualifier               => UserSegment.ALL
    case _: UserAllQualifier            => UserSegment.ALL
    case _                              => Segment.ALL
  }
}
