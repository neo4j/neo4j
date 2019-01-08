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
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

import org.neo4j.causalclustering.identity.StoreId;

public class GetStoreIdResponse
{
    private final StoreId storeId;

    GetStoreIdResponse( StoreId storeId )
    {
        this.storeId = storeId;
    }

    public StoreId storeId()
    {
        return storeId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        GetStoreIdResponse that = (GetStoreIdResponse) o;
        return Objects.equals( storeId, that.storeId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( storeId );
    }

    @Override
    public String toString()
    {
        return "GetStoreIdResponse{" +
                "storeId=" + storeId +
                '}';
    }
}
