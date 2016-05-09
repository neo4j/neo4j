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

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.RandomRule;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import static java.util.Arrays.asList;

import static org.neo4j.helpers.collection.Iterators.loop;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;

public class RelationshipTypeCheckerStepTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldReturnRelationshipTypeIdsInReverseOrderOfTokenCreation() throws Throwable
    {
        shouldReturnRelationshipTypeIdsInReverseOrderOfTokenCreation( true );
    }

    @Test
    public void shouldReturnRelationshipTypeNamesInReverseOrderOfTokenCreation() throws Throwable
    {
        shouldReturnRelationshipTypeIdsInReverseOrderOfTokenCreation( false );
    }

    private void shouldReturnRelationshipTypeIdsInReverseOrderOfTokenCreation( boolean typeIds ) throws Throwable
    {
        // GIVEN
        BatchingRelationshipTypeTokenRepository repository = mock( BatchingRelationshipTypeTokenRepository.class );
        RelationshipTypeCheckerStep step =
                new RelationshipTypeCheckerStep( mock( StageControl.class ), DEFAULT, repository );

        // WHEN
        Batch<InputRelationship,RelationshipRecord> relationships =
                batchOfRelationshipsWithRandomTypes( 10, typeIds );
        step.process( relationships, mock( BatchSender.class ) );
        step.done();

        // THEN
        Object[] processed = step.getRelationshipTypes( 100 );

        InOrder inOrder = inOrder( repository );
        for ( Object type : reversed( processed ) )
        {
            inOrder.verify( repository ).getOrCreateId( type );
        }
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldReturnRelationshipTypesInDescendingOrder() throws Throwable
    {
        // GIVEN
        BatchingRelationshipTypeTokenRepository repository = mock( BatchingRelationshipTypeTokenRepository.class );
        RelationshipTypeCheckerStep step = new RelationshipTypeCheckerStep( mock( StageControl.class ), DEFAULT,
                repository );
        Batch<InputRelationship,RelationshipRecord> relationships =
                batchOfRelationshipsWithRandomTypes( 10, true/*use the raw ids*/ );
        step.process( relationships, mock( BatchSender.class ) );

        // WHEN
        step.done();

        // THEN
        TreeSet<Integer> expected = idsOf( relationships );
        Object[] processed = step.getRelationshipTypes( 100 );
        int i = 0;
        for ( Object expectedType : loop( expected.descendingIterator() ) )
        {
            assertEquals( expectedType, processed[i++] );
        }
    }

    private TreeSet<Integer> idsOf( Batch<InputRelationship,RelationshipRecord> relationships )
    {
        TreeSet<Integer> types = new TreeSet<>();
        for ( InputRelationship relationship : relationships.input )
        {
            types.add( relationship.typeId() );
        }
        return types;
    }

    private Batch<InputRelationship,RelationshipRecord> batchOfRelationshipsWithRandomTypes(
            int maxTypes, boolean typeIds )
    {
        InputRelationship[] relationships = new InputRelationship[100];
        for ( int i = 0; i < relationships.length; i++ )
        {
            int typeId = random.nextInt( maxTypes );
            relationships[i] = new InputRelationship( "test", i, i, NO_PROPERTIES, null, GLOBAL,
                    0L, GLOBAL, 0L,
                    typeIds ? null : "TYPE_" + String.valueOf( typeId ),
                    typeIds ? typeId : null );
        }
        return new Batch<>( relationships );
    }

    private Object[] reversed( Object[] objects )
    {
        List<Object> list = new ArrayList<>( asList( objects ) );
        Collections.reverse( list );
        return list.toArray();
    }
}
