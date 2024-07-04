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
package org.neo4j.fabric.executor;

import java.util.Optional;
import java.util.UUID;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public interface Location {

    DatabaseReference databaseReference();

    long graphId();

    String getDatabaseName();

    default UUID getUuid() {
        return databaseReference().id();
    }

    /**
     * A Local location refers to a graph/database running on this instance of Neo4j.
     */
    record Local(long graphId, DatabaseReferenceImpl.Internal databaseReference) implements Location {
        @Override
        public String getDatabaseName() {
            return databaseReference.databaseId().name();
        }
    }

    /**
     * A Remote location refers to a graph/database running on another instance of Neo4j.
     * This instance may or may not be part of the same DBMS.
     */
    interface Remote extends Location {

        /**
         * A Remote.Internal location refers to a graph/database running on another instance of Neo4j within
         * the same DBMS.
         */
        interface Internal extends Remote {

            /**
             * A Remote.Internal.Routing location refers to a graph/database running on another instance of
             * Neo4j within the same DBMS.
             * This location does not indicate a concrete server and finding the servers with the graph/database
             * and choosing among them is left to the driver responsible for the in-cluster communication.
             */
            record Routing(long graphId, DatabaseReferenceImpl.Internal databaseReference)
                    implements Location.Remote.Internal {

                @Override
                public String getDatabaseName() {
                    return databaseReference.databaseId().name();
                }
            }

            /**
             * A Remote.Internal.Direct location refers to a graph/database running on a concrete instance of
             * Neo4j within the same DBMS.
             */
            record Direct(long graphId, DatabaseReferenceImpl.Internal databaseReference, RemoteUri uri)
                    implements Location.Remote.Internal {

                @Override
                public String getDatabaseName() {
                    return databaseReference.databaseId().name();
                }

                public RemoteUri getUri() {
                    return uri;
                }
            }
        }

        /**
         * A Remote.External location refers to a graph/database running on another instance of Neo4j, in another DBMS.
         */
        record External(long graphId, DatabaseReferenceImpl.External databaseReference) implements Location.Remote {

            @Override
            public String getDatabaseName() {
                return databaseReference.targetAlias().name();
            }

            public RemoteUri getUri() {
                return databaseReference.externalUri();
            }

            public String locationName() {
                return databaseReference.alias().name();
            }

            public Optional<String> locationNamespace() {
                return databaseReference.namespace().map(NormalizedDatabaseName::name);
            }
        }
    }
}
