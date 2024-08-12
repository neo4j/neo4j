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
package org.neo4j.internal.kernel.api.exceptions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;

public class DeletedNodeStillHasRelationshipsException extends ConstraintViolationTransactionFailureException {
    public DeletedNodeStillHasRelationshipsException(long nodeId) {
        super(
                "Cannot delete node<" + nodeId
                        + ">, because it still has relationships. To delete this node, you must first delete its relationships.");
    }

    public DeletedNodeStillHasRelationshipsException(ErrorGqlStatusObject gqlStatusObject, long nodeId) {
        super(
                gqlStatusObject,
                "Cannot delete node<" + nodeId
                        + ">, because it still has relationships. To delete this node, you must first delete its relationships.");
    }

    public DeletedNodeStillHasRelationshipsException() {
        super("Cannot delete node that was created in this transaction, because it still has relationships.");
    }

    public DeletedNodeStillHasRelationshipsException(ErrorGqlStatusObject gqlStatusObject) {
        super(
                gqlStatusObject,
                "Cannot delete node that was created in this transaction, because it still has relationships.");
    }
}
