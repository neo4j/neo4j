/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import java.util.Arrays;
import java.util.Comparator;

public class EntityTokenUpdate
{
    public static final Comparator<? super EntityTokenUpdate> SORT_BY_ENTITY_ID =
            Comparator.comparingLong( EntityTokenUpdate::getEntityId );

    private final long entityId;
    private final long[] tokensBefore;
    private final long[] tokensAfter;
    private final long txId;

    private EntityTokenUpdate( long entityId, long[] tokensBefore, long[] tokensAfter, long txId )
    {
        this.entityId = entityId;
        this.tokensBefore = tokensBefore;
        this.tokensAfter = tokensAfter;
        this.txId = txId;
    }

    public long getEntityId()
    {
        return entityId;
    }

    public long[] getTokensBefore()
    {
        return tokensBefore;
    }

    public long[] getTokensAfter()
    {
        return tokensAfter;
    }

    public long getTxId()
    {
        return txId;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[entity:" + entityId + ", tokensBefore:" + Arrays.toString( tokensBefore ) +
                ", tokensAfter:" + Arrays.toString( tokensAfter ) + "]";
    }

    public static EntityTokenUpdate tokenChanges( long entityId, long[] tokensBeforeChange, long[] tokensAfterChange )
    {
        return tokenChanges( entityId, tokensBeforeChange, tokensAfterChange, -1 );
    }

    public static EntityTokenUpdate tokenChanges( long entityId, long[] tokensBeforeChange, long[] tokensAfterChange, long txId )
    {
        return new EntityTokenUpdate( entityId, tokensBeforeChange, tokensAfterChange, txId );
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

        EntityTokenUpdate that = (EntityTokenUpdate) o;

        if ( entityId != that.entityId )
        {
            return false;
        }
        if ( !Arrays.equals( tokensAfter, that.tokensAfter ) )
        {
            return false;
        }
        return Arrays.equals( tokensBefore, that.tokensBefore );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (entityId ^ (entityId >>> 32));
        result = 31 * result + (tokensBefore != null ? Arrays.hashCode( tokensBefore ) : 0);
        result = 31 * result + (tokensAfter != null ? Arrays.hashCode( tokensAfter ) : 0);
        return result;
    }
}
