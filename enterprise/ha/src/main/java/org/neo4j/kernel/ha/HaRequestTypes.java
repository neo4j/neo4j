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
package org.neo4j.kernel.ha;

import org.neo4j.com.RequestType;

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

        public boolean is( RequestType type )
        {
            return type.id() == ordinal();
        }
    }

    RequestType type( Type type );

    RequestType type( byte id );
}
