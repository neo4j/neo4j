/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.RequestType;
import org.neo4j.kernel.ha.com.master.Master;

public interface HaRequestTypes
{
    enum Type
    {
        // Order here is vital since it represents the request type (byte) ordinal which is communicated
        // as part of the HA protocol.
        ALLOCATE_IDS,
        CREATE_RELATIONSHIP_TYPE,
        ACQUIRE_EXCLUSIVE_LOCK,
        ACQUIRE_SHARED_LOCK,
        COMMIT,
        PULL_UPDATES,
        END_LOCK_SESSION,
        HANDSHAKE,
        COPY_STORE,
        COPY_TRANSACTIONS,
        NEW_LOCK_SESSION,
        PUSH_TRANSACTIONS,
        CREATE_PROPERTY_KEY,
        CREATE_LABEL;

        public boolean is( RequestType<?> type )
        {
            return type.id() == ordinal();
        }
    }

    RequestType<Master> type( Type type );

    RequestType<Master> type( byte id );
}
