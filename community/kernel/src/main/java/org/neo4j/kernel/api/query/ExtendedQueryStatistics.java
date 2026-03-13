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
package org.neo4j.kernel.api.query;

import org.neo4j.graphdb.QueryStatistics;

/**
 * Extra query statistics collected by the Cypher runtimes that is not yet part of the public API
 */
public interface ExtendedQueryStatistics extends QueryStatistics {
    long nodesCreated();

    long nodesDeleted();

    long relationshipsCreated();

    long relationshipsDeleted();

    long propertiesSet();

    long labelsAdded();

    long labelsRemoved();

    long getTransactionsCommitted();

    long getTransactionsStarted();

    long getTransactionsRolledBack();

    long getFileLinesRead();

    ExtendedQueryStatistics EMPTY = new ExtendedQueryStatistics() {

        @Override
        public int getNodesCreated() {
            return 0;
        }

        @Override
        public int getNodesDeleted() {
            return 0;
        }

        @Override
        public int getRelationshipsCreated() {
            return 0;
        }

        @Override
        public int getRelationshipsDeleted() {
            return 0;
        }

        @Override
        public int getPropertiesSet() {
            return 0;
        }

        @Override
        public int getLabelsAdded() {
            return 0;
        }

        @Override
        public int getLabelsRemoved() {
            return 0;
        }

        @Override
        public int getIndexesAdded() {
            return 0;
        }

        @Override
        public int getIndexesRemoved() {
            return 0;
        }

        @Override
        public int getConstraintsAdded() {
            return 0;
        }

        @Override
        public int getConstraintsRemoved() {
            return 0;
        }

        @Override
        public int getSystemUpdates() {
            return 0;
        }

        @Override
        public boolean containsUpdates() {
            return false;
        }

        @Override
        public boolean containsSystemUpdates() {
            return false;
        }

        @Override
        public long nodesCreated() {
            return 0L;
        }

        @Override
        public long nodesDeleted() {
            return 0L;
        }

        @Override
        public long relationshipsCreated() {
            return 0L;
        }

        @Override
        public long relationshipsDeleted() {
            return 0L;
        }

        @Override
        public long propertiesSet() {
            return 0L;
        }

        @Override
        public long labelsAdded() {
            return 0L;
        }

        @Override
        public long labelsRemoved() {
            return 0L;
        }

        @Override
        public long getTransactionsCommitted() {
            return 0L;
        }

        @Override
        public long getTransactionsStarted() {
            return 0L;
        }

        @Override
        public long getTransactionsRolledBack() {
            return 0L;
        }

        @Override
        public long getFileLinesRead() {
            return 0L;
        }
    };
}
