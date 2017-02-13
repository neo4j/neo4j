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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.IOFunction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( Parameterized.class )
public class DatabaseCompositeIndexAccessorTest
{
    @Rule
    public final ThreadingRule threading = new ThreadingRule();
    @ClassRule
    public static final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Parameterized.Parameter
    public IOFunction<DirectoryFactory,LuceneIndexAccessor> accessorFactory;

    private LuceneIndexAccessor accessor;
    private final long nodeId = 1, nodeId2 = 2;
    private final Object[] value = new String[]{"value", "valuex"}, value2 = new Integer[]{40, 42};
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    private final NewIndexDescriptor index = NewIndexDescriptorFactory.forLabel( 0, 0 );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<IOFunction<DirectoryFactory,LuceneIndexAccessor>[]> implementations()
    {
        final File dir = new File( "dir" );
        return Arrays.asList(
                arg( dirFactory1 -> {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create()
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( dir )
                            .withIndexIdentifier( "1" )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index );
                } ),
                arg( dirFactory1 -> {
                    SchemaIndex index = LuceneSchemaIndexBuilder.create()
                            .uniqueIndex()
                            .withFileSystem( fileSystemRule.get() )
                            .withDirectoryFactory( dirFactory1 )
                            .withIndexRootFolder( dir )
                            .withIndexIdentifier( "testIndex" )
                            .build();

                    index.create();
                    index.open();
                    return new LuceneIndexAccessor( index );
                } )
        );
    }

    private static IOFunction<DirectoryFactory,LuceneIndexAccessor>[] arg(
            IOFunction<DirectoryFactory,LuceneIndexAccessor> foo )
    {
        return new IOFunction[]{foo};
    }

    @Before
    public void before() throws IOException
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = accessorFactory.apply( dirFactory );
    }

    @After
    public void after() throws IOException
    {
        accessor.close();
        dirFactory.close();
    }

    @Test
    public void indexReaderShouldSupportScan() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        PrimitiveLongIterator results = reader.scan();

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), PrimitiveLongCollections.toSet( results ) );
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader.seek( value ) ) );
        reader.close();
    }

    @Test
    public void indexReaderShouldSupportSeekOnCompositeIndexUpdatedWithTwoTransactions() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );
        updateAndCommit( asList( add( nodeId, value ), add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        PrimitiveLongIterator results = reader.scan();

        // THEN
        assertEquals( asSet( nodeId, nodeId2 ), PrimitiveLongCollections.toSet( results ) );
        assertEquals( asSet( nodeId ), PrimitiveLongCollections.toSet( reader.seek( value ) ) );
        reader.close();
    }

    private IndexEntryUpdate add( long nodeId, Object[] values )
    {
        return IndexEntryUpdate.add( nodeId, index, values );
    }

    private void updateAndCommit( List<IndexEntryUpdate> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( IndexEntryUpdate update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }

}
