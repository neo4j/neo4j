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
package org.neo4j.kernel.impl.factory;

import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.kernel.api.exceptions.Status;

public class ReadReplica implements AccessCapability {
    public static final ReadReplica INSTANCE = new ReadReplica();

    private ReadReplica() {}

    @Override
    public void assertCanWrite() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N08)
                .withClassification(ErrorClassification.CLIENT_ERROR)
                .build();
        throw new WriteOperationsNotAllowedException(
                gql,
                "No write operations are allowed on this database. This is a read only Neo4j instance.",
                Status.General.ForbiddenOnReadOnlyDatabase);
    }
}
