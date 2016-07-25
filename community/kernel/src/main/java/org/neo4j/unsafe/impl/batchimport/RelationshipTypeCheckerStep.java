/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

/**
 * Counts relationships per type to later be able to provide all types, even sorted in descending order
 * of number of relationships per type.
 */
public class RelationshipTypeCheckerStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private static final Comparator<Map.Entry<Object,AtomicLong>> SORT_BY_COUNT_DESC =
            (e1,e2) -> Long.compare( e2.getValue().get(), e1.getValue().get() );
    private static final Comparator<Map.Entry<Object,AtomicLong>> SORT_BY_ID_DESC =
            (e1,e2) -> Integer.compare( (Integer)e2.getKey(), (Integer)e1.getKey() );
    private final ConcurrentMap<Object,AtomicLong> allTypes = new ConcurrentHashMap<>();
    private final BatchingRelationshipTypeTokenRepository typeTokenRepository;
    private Map.Entry<Object,AtomicLong>[] sortedTypes;

    public RelationshipTypeCheckerStep( StageControl control, Configuration config,
            BatchingRelationshipTypeTokenRepository typeTokenRepository )
    {
        super( control, "TYPE", config, 0 );
        this.typeTokenRepository = typeTokenRepository;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        for ( InputRelationship relationship : batch.input )
        {
            Object type = relationship.typeAsObject();
            AtomicLong count = allTypes.get( type );
            if ( count == null )
            {
                AtomicLong existing = allTypes.putIfAbsent( type, count = new AtomicLong() );
                count = existing != null ? existing : count;
            }
            count.incrementAndGet();
        }
        sender.send( batch );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected void done()
    {
        sortedTypes = allTypes.entrySet().toArray( new Map.Entry[allTypes.size()] );
        if ( sortedTypes.length > 0 )
        {
            Comparator<Map.Entry<Object,AtomicLong>> comparator = sortedTypes[0].getKey() instanceof Integer ?
                    SORT_BY_ID_DESC : SORT_BY_COUNT_DESC;
            Arrays.sort( sortedTypes, comparator );
        }

        // Create the types in the reverse order of which is returned in getAllTypes()
        // Why do we do that? Well, it's so that the relationship groups can be created iteratively
        // and still keeping order of (ascending) type in its chains. Relationship groups have next pointers
        // and creating these groups while still adhering to principal of sequential I/O doesn't allow us
        // to go back and update a previous group to point to a next relationship group. This is why we
        // create the groups in ascending id order whereas next pointers will always point backwards to
        // lower ids (and therefore relationship type ids). This fulfills the constraint of having
        // relationship group record chains be in order of ascending relationship type.
        for ( int i = sortedTypes.length - 1; i >= 0; i-- )
        {
            typeTokenRepository.getOrCreateId( sortedTypes[i].getKey() );
        }
        super.done();
    }

    /**
     * Returns relationship types which have number of relationships equal to or lower than the given threshold.
     *
     * @param belowOrEqualToThreshold threshold where relationship types which have this amount of relationships
     * or less will be returned.
     * @return the order of which to order {@link InputRelationship} when importing relationships.
     * The order in which these relationships are returned will be the reverse order of relationship type ids.
     * There are two modes of relationship types here, one is user defined String where this step
     * have full control of assigning ids to those and will do so based on size of types. The other mode
     * is where types are given as ids straight away (as Integer) where the order is already set and so
     * the types will not be sorted by size (which is simply an optimization anyway).
     */
    public Object[] getRelationshipTypes( long belowOrEqualToThreshold )
    {
        List<Object> result = new ArrayList<>();
        for ( Map.Entry<Object,AtomicLong> candidate : sortedTypes )
        {
            if ( candidate.getValue().get() <= belowOrEqualToThreshold )
            {
                result.add( candidate.getKey() );
            }
        }

        return result.toArray();
    }
}
