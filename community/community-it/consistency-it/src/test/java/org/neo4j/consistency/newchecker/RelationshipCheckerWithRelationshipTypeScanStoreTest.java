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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.internal.index.label.TokenScanWriter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith( RandomExtension.class )
class RelationshipCheckerWithRelationshipTypeScanStoreTest extends CheckerTestBase
{
    private int type;
    private int lowerType;
    private int higherType;

    @Inject
    private RandomRule random;

    @Override
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        super.configure( builder );
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
    }

    @Override
    void initialData( KernelTransaction tx ) throws KernelException
    {
        lowerType = tx.tokenWrite().relationshipTypeGetOrCreateForName( "A" );
        type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "B" );
        higherType = tx.tokenWrite().relationshipTypeGetOrCreateForName( "C" );
        assertThat( lowerType ).isLessThan( type );
        assertThat( type ).isLessThan( higherType );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void testShouldNotReportAnythingIfDataIsCorrect( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> createCompleteEntry( writer, type ),
                (Consumer<ConsistencyReport.RelationshipTypeScanConsistencyReport>[]) null
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexPointToRelationshipNotInUse( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry( type );
                    notInUse( relationshipId );
                    createIndexEntry( writer, relationshipId, type );
                },
                reporter -> reporter.relationshipNotInUse( any() )
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexHasHigherTypeThanRelationshipInStore( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry( type );
                    createIndexEntry( writer, relationshipId, higherType );
                },
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType( any(), anyLong() ),
                reporter -> reporter.relationshipTypeNotInIndex( any(), anyLong() )
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexHasLowerTypeThanRelationshipInStore( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry( type );
                    createIndexEntry( writer, relationshipId, lowerType );
                },
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType( any(), anyLong() ),
                reporter -> reporter.relationshipTypeNotInIndex( any(), anyLong() )
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexIsMissingEntryForRelationshipInUse( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> createStoreEntry( type ),
                reporter -> reporter.relationshipTypeNotInIndex( any(), anyLong() )
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexHasMultipleTypesForSameRelationshipOneCorrect( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry( type );
                    createIndexEntry( writer, relationshipId, type );
                    createIndexEntry( writer, relationshipId, lowerType );
                    createIndexEntry( writer, relationshipId, higherType );
                },
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType( any(), anyLong() )
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexHasMultipleTypesForSameRelationshipNoneCorrect( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry( type );
                    createIndexEntry( writer, relationshipId, lowerType );
                    createIndexEntry( writer, relationshipId, higherType );
                },
                reporter -> reporter.relationshipTypeNotInIndex( any(), anyLong() ),
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType( any(), anyLong() )
        );
    }

    @ParameterizedTest
    @EnumSource( Density.class )
    void indexHasMultipleTypesForSameRelationshipNotInUse( Density density ) throws Exception
    {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry( type );
                    notInUse( relationshipId );
                    createIndexEntry( writer, relationshipId, type );
                    createIndexEntry( writer, relationshipId, lowerType );
                    createIndexEntry( writer, relationshipId, higherType );
                },
                reporter -> reporter.relationshipNotInUse( any() )
        );
    }

    @Test
    void storeHasBigGapButIndexDoesNot() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx();
              TokenScanWriter writer = relationshipTypeIndex.newWriter( PageCursorTracer.NULL ) )
        {
            for ( int i = 0; i < 2 * IDS_PER_CHUNK; i++ )
            {
                if ( i == 0 )
                {
                    createCompleteEntry( writer, type );
                }
                else if ( i == 10 )
                {
                    long relationshipId = createStoreEntry( type );
                    notInUse( relationshipId );
                    createIndexEntry( writer, relationshipId, type );
                }
                else if ( i == IDS_PER_CHUNK - 1 )
                {
                    createCompleteEntry( writer, type );
                }
                else
                {
                    long relationshipId = createStoreEntry( type );
                    notInUse( relationshipId );
                }
            }

            tx.commit();
        }

        // when
        check();

        // then
        expect( ConsistencyReport.RelationshipTypeScanConsistencyReport.class, reporter -> reporter.relationshipNotInUse( any() ) );
    }

    @Test
    void shouldNotCheckRelationshipTypeScanStoreIfNotConfiguredTo() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            createStoreEntry( type );
            tx.commit();
        }

        // when
        ConsistencyFlags flags = new ConsistencyFlags( true, true, true, true, false, true );
        CheckerContext checkerContext = context( flags );
        check( checkerContext );

        // then
        verifyNoInteractions( monitor );
    }

    @SafeVarargs
    private void doVerifyCorrectReport( Density density, ThrowingConsumer<TokenScanWriter,IOException> targetRelationshipAction,
            Consumer<ConsistencyReport.RelationshipTypeScanConsistencyReport>... expectedCalls ) throws Exception
    {
        double recordFrequency = densityAsFrequency( density );
        int nbrOfRelationships = random.nextInt( 1, 2 * IDS_PER_CHUNK );
        int targetRelationshipRelationship = random.nextInt( nbrOfRelationships );

        // given
        try ( Transaction tx = db.beginTx();
              TokenScanWriter writer = relationshipTypeIndex.newWriter( PageCursorTracer.NULL ) )
        {
            for ( int i = 0; i < nbrOfRelationships; i++ )
            {
                if ( i == targetRelationshipRelationship )
                {
                    targetRelationshipAction.accept( writer );
                }
                else
                {
                    if ( random.nextDouble() < recordFrequency )
                    {
                        createCompleteEntry( writer, type );
                    }
                    else
                    {
                        notInUse( createStoreEntry( type ) );
                    }
                }
            }

            tx.commit();
        }

        // when
        ConsistencyFlags flags = new ConsistencyFlags( true, true, true, true, true, true );
        check( context( flags ) );

        // then
        if ( expectedCalls != null )
        {
            for ( Consumer<ConsistencyReport.RelationshipTypeScanConsistencyReport> expectedCall : expectedCalls )
            {
                expect( ConsistencyReport.RelationshipTypeScanConsistencyReport.class, expectedCall );
            }
        }
        else
        {
            verifyNoInteractions( monitor );
        }
    }

    private double densityAsFrequency( Density density )
    {
        double recordFrequency;
        switch ( density )
        {
        case DENSE:
            recordFrequency = 1;
            break;
        case SPARSE:
            recordFrequency = 0;
            break;
        case RANDOM:
            recordFrequency = random.nextDouble();
            break;
        default:
            throw new IllegalArgumentException( "Don't recognize density " + density );
        }
        return recordFrequency;
    }

    private void createCompleteEntry( TokenScanWriter writer, int type ) throws IOException
    {
        long relationshipId = createStoreEntry( type );
        createIndexEntry( writer, relationshipId, type );
    }

    private void createIndexEntry( TokenScanWriter writer, long relationshipId, int type ) throws IOException
    {
        writer.write( EntityTokenUpdate.tokenChanges( relationshipId, new long[0], new long[]{type} ) );
    }

    private long createStoreEntry( int type )
    {
        long relationship = relationshipStore.nextId( PageCursorTracer.NULL );
        long node1 = nodePlusCached( nodeStore.nextId( PageCursorTracer.NULL ), NULL, relationship );
        long node2 = nodePlusCached( nodeStore.nextId( PageCursorTracer.NULL ), NULL, relationship );
        relationship( relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, true );
        return relationship;
    }

    private void notInUse( long relationshipId )
    {
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        relationshipStore.getRecord( relationshipId, relationshipRecord, RecordLoad.NORMAL, PageCursorTracer.NULL );
        relationshipRecord.setInUse( false );
        relationshipStore.updateRecord( relationshipRecord, PageCursorTracer.NULL );
    }

    private void check() throws Exception
    {
        check( context() );
    }

    private void check( CheckerContext context ) throws Exception
    {
        new RelationshipChecker( context, noMandatoryProperties ).check( LongRange.range( 0, nodeStore.getHighId() ), true, true );
    }

    private enum Density
    {
        DENSE,
        SPARSE,
        RANDOM;
    }
}
