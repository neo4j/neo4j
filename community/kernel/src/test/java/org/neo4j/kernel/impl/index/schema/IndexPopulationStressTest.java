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
package org.neo4j.kernel.impl.index.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Value;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

public abstract class IndexPopulationStressTest
{
    @Rule
    public PageCacheAndDependenciesRule rules =
            new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, this.getClass() );

    protected final SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( 0, 0 );

    private IndexPopulator populator;

    /**
     * Create the provider to stress.
     */
    abstract IndexProvider newProvider( IndexDirectoryStructure.Factory directory );

    /**
     * Generate a random value to populate the index with.
     */
    abstract Value randomValue( Random random );

    @Before
    public void setup() throws IOException
    {
        File storeDir = rules.directory().graphDbDir();
        IndexDirectoryStructure.Factory directory =
                directoriesBySubProvider( directoriesByProvider( storeDir ).forProvider( new IndexProvider.Descriptor( "provider", "1.0" ) ) );

        IndexProvider indexProvider = newProvider( directory );

        rules.fileSystem().mkdirs( indexProvider.directoryStructure().rootDirectory() );

        populator = indexProvider.getPopulator( 0, descriptor, new IndexSamplingConfig( 1000, 0.2, true ) );
    }

    @After
    public void teardown() throws IOException
    {
        populator.close( true );
    }

    @Test
    public void stressIt() throws Throwable
    {
        Race race = new Race();
        final int threads = 50;
        final AtomicBoolean end = new AtomicBoolean();

        populator.create();

        for ( int i = 0; i < threads; i++ )
        {
            race.addContestant( Race.throwing( () ->
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( !end.get() )
                {
                    populator.add( randomAdds( random ) );
                }
            } ) );
        }

        try
        {
            race.go( 5, SECONDS );
            fail( "Race should have timed out." );
        }
        catch ( TimeoutException t )
        {
            // great, nothing else failed!
            end.set( true );
        }
    }

    private Collection<? extends IndexEntryUpdate<?>> randomAdds( Random random )
    {
        int n = random.nextInt( 100 ) + 1;
        List<IndexEntryUpdate<?>> updates = new ArrayList<>( n );
        for ( int i = 0; i < n; i++ )
        {
            Value value = randomValue( random );
            updates.add( IndexEntryUpdate.add( i, descriptor, value ) );
        }
        return updates;
    }
}
