/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StandaloneDynamicRecordAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.test.Randoms;
import org.neo4j.values.storable.Values;

import static java.lang.Long.max;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;

public class LimitedRecordGenerators implements RecordGenerators
{
    static final long NULL = -1;

    private final Randoms random;
    private final int entityBits;
    private final int propertyBits;
    private final int nodeLabelBits;
    private final int tokenBits;
    private final long nullValue;
    private final float fractionNullValues;

    public LimitedRecordGenerators( Randoms random, int entityBits, int propertyBits, int nodeLabelBits,
            int tokenBits, long nullValue )
    {
        this( random, entityBits, propertyBits, nodeLabelBits, tokenBits, nullValue, 0.2f );
    }

    public LimitedRecordGenerators( Randoms random, int entityBits, int propertyBits, int nodeLabelBits,
            int tokenBits, long nullValue, float fractionNullValues )
    {
        this.random = random;
        this.entityBits = entityBits;
        this.propertyBits = propertyBits;
        this.nodeLabelBits = nodeLabelBits;
        this.tokenBits = tokenBits;
        this.nullValue = nullValue;
        this.fractionNullValues = fractionNullValues;
    }

    @Override
    public Generator<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return ( recordSize, format, recordId ) -> new RelationshipTypeTokenRecord( toIntExact( recordId ) ).initialize(
                random.nextBoolean(),
                randomInt( tokenBits ) );
    }

    @Override
    public Generator<RelationshipGroupRecord> relationshipGroup()
    {
        return ( recordSize, format, recordId ) -> new RelationshipGroupRecord( recordId ).initialize(
                random.nextBoolean(),
                randomInt( tokenBits ),
                randomLongOrOccasionallyNull( entityBits ),
                randomLongOrOccasionallyNull( entityBits ),
                randomLongOrOccasionallyNull( entityBits ),
                randomLongOrOccasionallyNull( entityBits ),
                randomLongOrOccasionallyNull( entityBits ) );
    }

    @Override
    public Generator<RelationshipRecord> relationship()
    {
        return ( recordSize, format, recordId ) -> new RelationshipRecord( recordId ).initialize(
                random.nextBoolean(),
                randomLongOrOccasionallyNull( propertyBits ),
                random.nextLong( entityBits ), random.nextLong( entityBits ), randomInt( tokenBits ),
                randomLongOrOccasionallyNull( entityBits ), randomLongOrOccasionallyNull( entityBits ),
                randomLongOrOccasionallyNull( entityBits ), randomLongOrOccasionallyNull( entityBits ),
                random.nextBoolean(), random.nextBoolean() );
    }

    @Override
    public Generator<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return ( recordSize, format, recordId ) -> new PropertyKeyTokenRecord( toIntExact( recordId ) ).initialize(
                random.nextBoolean(),
                random.nextInt( tokenBits ),
                abs( random.nextInt() ) );
    }

    @Override
    public Generator<PropertyRecord> property()
    {
        return ( recordSize, format, recordId ) ->
        {
            PropertyRecord record = new PropertyRecord( recordId );
            int maxProperties = random.intBetween( 1, 4 );
            StandaloneDynamicRecordAllocator stringAllocator = new StandaloneDynamicRecordAllocator();
            StandaloneDynamicRecordAllocator arrayAllocator = new StandaloneDynamicRecordAllocator();
            record.setInUse( true );
            int blocksOccupied = 0;
            for ( int i = 0; i < maxProperties && blocksOccupied < 4; )
            {
                PropertyBlock block = new PropertyBlock();
                // Dynamic records will not be written and read by the property record format,
                // that happens in the store where it delegates to a "sub" store.
                PropertyStore.encodeValue( block, random.nextInt( tokenBits ), Values.of( random.propertyValue() ),
                        stringAllocator, arrayAllocator, true );
                int tentativeBlocksWithThisOne = blocksOccupied + block.getValueBlocks().length;
                if ( tentativeBlocksWithThisOne <= 4 )
                {
                    record.addPropertyBlock( block );
                    blocksOccupied = tentativeBlocksWithThisOne;
                }
            }
            record.setPrevProp( randomLongOrOccasionallyNull( propertyBits ) );
            record.setNextProp( randomLongOrOccasionallyNull( propertyBits ) );
            return record;
        };
    }

    @Override
    public Generator<NodeRecord> node()
    {
        return ( recordSize, format, recordId ) -> new NodeRecord( recordId ).initialize(
                random.nextBoolean(),
                randomLongOrOccasionallyNull( propertyBits ),
                random.nextBoolean(),
                randomLongOrOccasionallyNull( entityBits ),
                randomLongOrOccasionallyNull( nodeLabelBits, 0 ) );
    }

    @Override
    public Generator<LabelTokenRecord> labelToken()
    {
        return ( recordSize, format, recordId ) -> new LabelTokenRecord( toIntExact( recordId ) ).initialize(
                random.nextBoolean(),
                random.nextInt( tokenBits ) );
    }

    @Override
    public Generator<DynamicRecord> dynamic()
    {
        return ( recordSize, format, recordId ) ->
        {
            int dataSize = recordSize - format.getRecordHeaderSize();
            int length = random.nextBoolean() ? dataSize : random.nextInt( dataSize );
            long next = length == dataSize ? randomLong( propertyBits ) : nullValue;
            DynamicRecord record = new DynamicRecord( max( 1, recordId ) ).initialize( random.nextBoolean(),
                    random.nextBoolean(), next, random.nextInt( PropertyType.values().length ), length );
            byte[] data = new byte[record.getLength()];
            random.nextBytes( data );
            record.setData( data );
            return record;
        };
    }

    private int randomInt( int maxBits )
    {
        int bits = random.nextInt( maxBits + 1 );
        int max = 1 << bits;
        return random.nextInt( max );
    }

    private long randomLong( int maxBits )
    {
        int bits = random.nextInt( maxBits + 1 );
        long max = 1L << bits;
        return random.nextLong( max );
    }

    private long randomLongOrOccasionallyNull( int maxBits )
    {
        return randomLongOrOccasionallyNull( maxBits, NULL );
    }

    private long randomLongOrOccasionallyNull( int maxBits, long nullValue )
    {
        return random.nextFloat() < fractionNullValues ? nullValue : randomLong( maxBits );
    }
}
