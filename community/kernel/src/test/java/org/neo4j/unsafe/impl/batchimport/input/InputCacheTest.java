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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.neo4j.io.ByteUnit.kibiBytes;

public class InputCacheTest
{
    private static final String[] TOKENS = new String[] {"One", "Two", "Three", "Four", "Five", "Six", "Seven"};
    private static final int countPerThread = 10_000;
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory dir = TestDirectory.testDirectory();
    private final RandomRule randomRule = new RandomRule().withSeedForAllTests( 1515752471383L );
    private final int threads = Runtime.getRuntime().availableProcessors();
    private final ExecutorService executor = Executors.newFixedThreadPool( threads );
    private final List<Future<?>> futures = new ArrayList<>();
    private final int totalCount = threads * countPerThread;
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( dir ).around( randomRule ).around( fileSystemRule );

    @Test
    public void shouldCacheAndRetrieveNodes() throws Exception
    {
        // GIVEN
        try ( InputCache cache =
                new InputCache( fileSystemRule.get(), dir.directory(), Standard.LATEST_RECORD_FORMATS, (int) kibiBytes( 8 ) ) )
        {
            try ( InputCacher cacher = cache.cacheNodes() )
            {
                writeEntities( cacher, this::randomNode );
            }

            // WHEN/THEN
            try ( InputIterator reader = cache.nodes().iterator() )
            {
                List<InputEntity> allReadEntities = readEntities( reader );
                assertEquals( totalCount, allReadEntities.size() );
                executor.shutdown();
            }
        }
        assertNoFilesLeftBehind();
    }

    @Test
    public void shouldCacheAndRetrieveRelationships() throws Exception
    {
        // GIVEN
        try ( InputCache cache =
                new InputCache( fileSystemRule.get(), dir.directory(), Standard.LATEST_RECORD_FORMATS, 200 ) )
        {
            try ( InputCacher cacher = cache.cacheRelationships() )
            {
                writeEntities( cacher, this::randomRelationship );
            }

            // WHEN/THEN
            try ( InputIterator reader = cache.relationships().iterator() )
            {
                List<InputEntity> allReadEntities = readEntities( reader );
                assertEquals( totalCount, allReadEntities.size() );
                executor.shutdown();
            }
        }
        assertNoFilesLeftBehind();
    }

    private List<InputEntity> readEntities( InputIterator reader ) throws Exception
    {
        for ( int i = 0; i < threads; i++ )
        {
            submit( () ->
            {
                List<InputEntity> entities = new ArrayList<>();
                try ( InputChunk chunk = reader.newChunk() )
                {
                    while ( reader.next( chunk ) )
                    {
                        InputEntity entity = new InputEntity();
                        while ( chunk.next( entity ) )
                        {
                            entities.add( entity );
                            entity = new InputEntity();
                        }
                    }
                }
                return entities;
            } );
        }
        List<InputEntity> allReadEntities = new ArrayList<>();
        this.<List<InputEntity>>results( allReadEntities::addAll );
        return allReadEntities;
    }

    private void writeEntities( InputCacher cacher, BiConsumer<Randoms,InputEntityVisitor> generator ) throws Exception
    {
        for ( int i = 0; i < threads; i++ )
        {
            Randoms localRandom = new Randoms( new Random( randomRule.seed() + i ), Randoms.DEFAULT );
            submit( () ->
            {
                InputEntity actual = new InputEntity();
                try ( InputEntityVisitor local = cacher.wrap( actual ) )
                {
                    for ( int j = 0; j < countPerThread; j++ )
                    {
                        generator.accept( localRandom, local );
                    }
                }
                return null;
            } );
        }
        results( ignore ->
        {
            /*just await them*/
        } );
    }

    private void assertNoFilesLeftBehind()
    {
        assertEquals( 0, fileSystemRule.get().listFiles( dir.directory() ).length );
    }

    private void randomRelationship( Randoms random, InputEntityVisitor relationship )
    {
        if ( random.random().nextFloat() < 0.1f )
        {
            relationship.type( abs( random.random().nextInt( 20_000 ) ) );
            relationship.propertyId( abs( random.random().nextLong() ) );
        }
        else
        {
            relationship.type( randomType( random ) );
            randomProperties( relationship, random );
        }
        relationship.startId( randomId( random ), randomGroup( random ) );
        relationship.endId( randomId( random ), randomGroup( random ) );
        try
        {
            relationship.endOfEntity();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void randomNode( Randoms random, InputEntityVisitor node )
    {
        if ( random.random().nextFloat() < 0.1f )
        {
            node.id( randomId( random ) );
            node.propertyId( randomId( random ) );
            node.labelField( randomId( random ) );
        }
        else
        {
            node.id( randomId( random ), randomGroup( random ) );
            randomProperties( node, random );
            node.labels( randomLabels( random ) );
        }
        try
        {
            node.endOfEntity();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void randomProperties( InputEntityVisitor entity, Randoms random )
    {
        int length = random.random().nextInt( 10 );
        for ( int i = 0; i < length; i++ )
        {
            Object value = random.propertyValue();
            if ( random.random().nextFloat() < 0.2f )
            {
                entity.property( random.intBetween( 0, 10 ), value );
            }
            else
            {
                entity.property( random.among( TOKENS ), value );
            }
        }
    }

    private String randomType( Randoms random )
    {
        return random.among( TOKENS );
    }

    private Group randomGroup( Randoms random )
    {
        return new Group.Adapter( random.nextInt( 100 ), random.string() );
    }

    private String[] randomLabels( Randoms random )
    {
        return random.selection( TOKENS, 1, 5, false );
    }

    private long randomId( Randoms random )
    {
        return abs( random.random().nextLong() );
    }

    private void submit( Callable<?> toRun )
    {
        futures.add( executor.submit( toRun ) );
    }

    @SuppressWarnings( "unchecked" )
    private <T> void results( Consumer<T> consumer ) throws Exception
    {
        for ( Future<?> future : futures )
        {
            consumer.accept( (T) future.get() );
        }
        futures.clear();
    }
}
