/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.util.Collections;

import org.junit.Test;

import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.util.FailureStorage;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.api.impl.index.AllNodesCollector.getAllNodes;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class DeferredConstraintVerificationUniqueLuceneIndexPopulatorTest
{
    @Test
    public void shouldVerifyThatThereAreNoDuplicates() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );
        populator.add( 2, "value2" );
        populator.add( 3, "value3" );

        // when
        populator.verifyDeferredConstraints();
        populator.close( true );

        // then
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 2l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
        assertEquals( asList( 3l ), getAllNodes( directoryFactory, indexDirectory, "value3" ) );
    }

    @Test
    public void shouldUpdateEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater();

        updater.process( NodePropertyUpdate.change( 1, 1, "value1", new long[]{}, "value2", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
    }

    @Test
    public void shouldUpdateEntryForNodeThatHasPropertyRemovedAndThenAddedAgain() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater();

        updater.process( NodePropertyUpdate.remove( 1, 1, "value1", new long[]{} ) );
        updater.process( NodePropertyUpdate.add( 1, 1, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( asList(1l), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
    }

    @Test
    public void shouldRemoveEntryForNodeThatHasAlreadyBeenIndexed() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater();

        updater.process( NodePropertyUpdate.remove( 1, 1, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( Collections.EMPTY_LIST, getAllNodes( directoryFactory, indexDirectory, "value1" ) );
    }

    @Test
    public void shouldBeAbleToHandleSwappingOfIndexValues() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        // when
        IndexUpdater updater = populator.newPopulatingUpdater();

        updater.process( NodePropertyUpdate.change( 1, 1, "value1", new long[]{}, "value2", new long[]{} ) );
        updater.process( NodePropertyUpdate.change( 2, 1, "value2", new long[]{}, "value1", new long[]{} ) );

        populator.close( true );

        // then
        assertEquals( asList( 2l ), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
    }

    @Test
    public void shouldFailAtVerificationStageWithAlreadyIndexedStringValue() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );
        populator.add( 2, "value2" );
        populator.add( 3, "value1" );

        // when
        try
        {
            populator.verifyDeferredConstraints();

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldFailAtVerificationStageWithAlreadyIndexedNumberValue() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, 1 );
        populator.add( 2, 2 );
        populator.add( 3, 1 );

        // when
        try
        {
            populator.verifyDeferredConstraints();

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( 1.0, conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectDuplicateEntryWhenUsingPopulatingUpdater() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        populator.add( 1, "value1" );
        populator.add( 2, "value2" );

        // when
        try
        {
            IndexUpdater updater = populator.newPopulatingUpdater();
            updater.process( NodePropertyUpdate.add( 3, 1, "value1", new long[]{} ) );
            updater.close();

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 3, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldRejectDuplicateEntryAfterUsingPopulatingUpdater() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        IndexUpdater updater = populator.newPopulatingUpdater();
        updater.process( NodePropertyUpdate.add( 1, 1, "value1", new long[]{} ) );
        populator.add( 2, "value1" );

        // when
        try
        {
            populator.verifyDeferredConstraints();

            fail( "should have thrown exception" );
        }
        // then
        catch ( PreexistingIndexEntryConflictException conflict )
        {
            assertEquals( 1, conflict.getExistingNodeId() );
            assertEquals( "value1", conflict.getPropertyValue() );
            assertEquals( 2, conflict.getAddedNodeId() );
        }
    }

    @Test
    public void shouldNotRejectDuplicateEntryOnSameNodeIdAfterUsingPopulatingUpdater() throws Exception
    {
        // given
        File indexDirectory = new File( "target/whatever" );
        final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
        DeferredConstraintVerificationUniqueLuceneIndexPopulator populator = new
                DeferredConstraintVerificationUniqueLuceneIndexPopulator(
                documentLogic, standard(),
                new IndexWriterStatus(), directoryFactory, indexDirectory, failureStorage, indexId );
        populator.create();

        IndexUpdater updater = populator.newPopulatingUpdater();
        updater.process( NodePropertyUpdate.add( 1, 1, "value1", new long[]{} ) );
        updater.process( NodePropertyUpdate.change( 1, 1, "value1", new long[]{}, "value1", new long[]{} ) );
        updater.close();
        populator.add( 2, "value2" );
        populator.add( 3, "value3" );

        // when
        populator.verifyDeferredConstraints();
        populator.close( true );

        // then
        assertEquals( asList( 1l ), getAllNodes( directoryFactory, indexDirectory, "value1" ) );
        assertEquals( asList( 2l ), getAllNodes( directoryFactory, indexDirectory, "value2" ) );
        assertEquals( asList( 3l ), getAllNodes( directoryFactory, indexDirectory, "value3" ) );
    }

    private final FailureStorage failureStorage = mock( FailureStorage.class );
    private final long indexId = 1;
    private DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
}
