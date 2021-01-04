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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import org.neo4j.common.EntityType;

import static java.lang.Math.toIntExact;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.index.schema.TokenScanValue.RANGE_SIZE;

/**
 * Represents a range of entities and token ids attached to those entities. All entities in the range are present in
 * {@link #entities() entities array}, but not all entity ids will have corresponding {@link #tokens(long) tokens},
 * where an empty long[] will be returned instead.
 */
public class EntityTokenRange
{
    public static final long[][] NO_TOKENS = new long[RANGE_SIZE][];
    private final long idRange;
    private final long[] entities;
    private final long[][] tokens;
    private final EntityType entityType;
    private final long lowRangeId;
    private final long highRangeId;

    /**
     * @param idRange entity id range, e.g. in which id span the entities are.
     * @param tokens long[][] where first dimension is relative entity id in this range, i.e. 0-rangeSize
     * and second the token ids for that entity, potentially empty if there are none for that entity.
     * The first dimension must be the size of the range.
     */
    public EntityTokenRange( long idRange, long[][] tokens, EntityType entityType )
    {
        this.idRange = idRange;
        this.tokens = tokens;
        this.entityType = entityType;
        int rangeSize = tokens.length;
        this.lowRangeId = idRange * rangeSize;
        this.highRangeId = lowRangeId + rangeSize - 1;

        this.entities = new long[rangeSize];
        for ( int i = 0; i < rangeSize; i++ )
        {
            entities[i] = lowRangeId + i;
        }
    }

    /**
     * @return the range id of this range. This is the base entity id divided by range size.
     * Example: A store with entities 1,3,20,22 and a range size of 16 would return ranges:
     * - rangeId=0, entities=1,3
     * - rangeId=1, entities=20,22
     */
    public long id()
    {
        return idRange;
    }

    public boolean covers( long entityId )
    {
        return entityId >= lowRangeId && entityId <= highRangeId;
    }

    public boolean isBelow( long entityId )
    {
        return highRangeId < entityId;
    }

    /**
     * @return entity ids in this range, the entities in this array may or may not have {@link #tokens(long) tokens}
     * attached to it.
     */
    public long[] entities()
    {
        return entities;
    }

    /**
     * Returns the token ids (as longs) for the given entity id. The {@code entityId} must be one of the ids
     * from {@link #entities()}.
     *
     * @param entityId the entity id to return tokens for.
     * @return token ids for the given {@code entityId}.
     */
    public long[] tokens( long entityId )
    {
        int index = toIntExact( entityId - lowRangeId );
        assert index >= 0 && index < tokens.length : "entityId:" + entityId + ", idRange:" + idRange;
        return tokens[index] != null ? tokens[index] : EMPTY_LONG_ARRAY;
    }

    private static String toString( String prefix, long[] entities, long[][] tokens )
    {
        StringBuilder result = new StringBuilder( prefix );
        result.append( "; {" );
        for ( int i = 0; i < entities.length; i++ )
        {
            if ( i != 0 )
            {
                result.append( ", " );
            }
            result.append( "Entity[" ).append( entities[i] ).append( "]: Tokens[" );
            String sep = "";
            if ( tokens[i] != null )
            {
                for ( long tokenId : tokens[i] )
                {
                    result.append( sep ).append( tokenId );
                    sep = ", ";
                }
            }
            else
            {
                result.append( "null" );
            }
            result.append( ']' );
        }
        return result.append( "}]" ).toString();
    }

    @Override
    public String toString()
    {
        String rangeName = entityType == EntityType.NODE ? "NodeLabelRange" : "RelationshipTypeRange";
        String rangeString = lowRangeId + "-" + (highRangeId + 1);
        String prefix = rangeName + "[idRange=" + rangeString;
        return toString( prefix, entities, tokens );
    }

    static void readBitmap( long bitmap, long tokenId, MutableLongList[] tokensPerEntity )
    {
        while ( bitmap != 0 )
        {
            int relativeEntityId = Long.numberOfTrailingZeros( bitmap );
            if ( tokensPerEntity[relativeEntityId] == null )
            {
                tokensPerEntity[relativeEntityId] = new LongArrayList();
            }
            tokensPerEntity[relativeEntityId].add( tokenId );
            bitmap &= bitmap - 1;
        }
    }

    static long[][] convertState( LongList[] state )
    {
        long[][] tokenIdsByEntityIndex = new long[state.length][];
        for ( int i = 0; i < state.length; i++ )
        {
            final LongList tokenIdList = state[i];
            if ( tokenIdList != null )
            {
                tokenIdsByEntityIndex[i] = tokenIdList.toArray();
            }
        }
        return tokenIdsByEntityIndex;
    }
}
