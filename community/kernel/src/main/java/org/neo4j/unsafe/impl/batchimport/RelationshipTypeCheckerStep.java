/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

import static java.lang.Thread.currentThread;

/**
 * Counts relationships per type to later be able to provide all types, even sorted in descending order
 * of number of relationships per type.
 */
public class RelationshipTypeCheckerStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private static final Function<Object,MutableLong> NEW_MUTABLE_LONG = type -> new MutableLong();
    private static final Comparator<Map.Entry<Object,MutableLong>> SORT_BY_COUNT_DESC =
            ( e1, e2 ) -> Long.compare( e2.getValue().longValue(), e1.getValue().longValue() );
    private static final Comparator<Map.Entry<Object,MutableLong>> SORT_BY_ID_DESC =
            ( e1, e2 ) -> Integer.compare( (Integer) e2.getKey(), (Integer) e1.getKey() );
    private final Map<Thread,Map<Object,MutableLong>> typeCheckers = new ConcurrentHashMap<>();
    private final BatchingRelationshipTypeTokenRepository typeTokenRepository;
    private RelationshipTypeDistribution distribution;

    public RelationshipTypeCheckerStep( StageControl control, Configuration config,
            BatchingRelationshipTypeTokenRepository typeTokenRepository )
    {
        super( control, "TYPE", config, 0 );
        this.typeTokenRepository = typeTokenRepository;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        Map<Object,MutableLong> typeMap = typeCheckers.computeIfAbsent( currentThread(), t -> new HashMap<>() );
        Stream.of( batch.input )
              .map( InputRelationship::typeAsObject )
              .filter( type -> type != null )
              .forEach( type -> typeMap.computeIfAbsent( type, NEW_MUTABLE_LONG ).increment() );
        sender.send( batch );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected void done()
    {
        Map<Object,MutableLong> mergedTypes = new HashMap<>();
        typeCheckers.forEach( ( thread, localTypes ) ->
            localTypes.forEach( ( type, localCount ) ->
                mergedTypes.computeIfAbsent( type, t -> new MutableLong() ).add( localCount.longValue() ) ) );

        Map.Entry<Object,MutableLong>[] sortedTypes = mergedTypes.entrySet().toArray( new Map.Entry[mergedTypes.size()] );
        if ( sortedTypes.length > 0 )
        {
            Comparator<Map.Entry<Object,MutableLong>> comparator = sortedTypes[0].getKey() instanceof Integer ?
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
        distribution = new RelationshipTypeDistribution( sortedTypes );
        super.done();
    }

    public RelationshipTypeDistribution getDistribution()
    {
        return distribution;
    }
}
