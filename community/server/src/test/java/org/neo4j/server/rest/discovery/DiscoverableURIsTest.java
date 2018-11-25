/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.rest.discovery;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.BiConsumer;

import org.neo4j.graphdb.config.InvalidSettingException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DiscoverableURIsTest
{
    private BiConsumer<String,URI> consumer = mock( BiConsumer.class );

    @Test
    public void shouldNotInvokeConsumerWhenEmpty()
    {
        DiscoverableURIs empty = new DiscoverableURIs.Builder().build();

        empty.forEach( consumer );

        verify( consumer, never() ).accept( anyString(), any() );
    }

    @Test
    public void shouldInvokeConsumerForEachKey() throws URISyntaxException
    {
        DiscoverableURIs discoverables =
                new DiscoverableURIs.Builder()
                        .add( "a", "/test", DiscoverableURIs.NORMAL )
                        .add( "b", "/data", DiscoverableURIs.NORMAL )
                        .add( "c", "http://www.example.com", DiscoverableURIs.LOW )
                        .build();

        discoverables.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", new URI( "/test" ) );
        verify( consumer, times( 1 ) ).accept( "b", new URI( "/data" ) );
        verify( consumer, times( 1 ) ).accept( "c", new URI( "http://www.example.com" ) );
    }

    @Test
    public void shouldThrowWhenAddingTwoEntriesWithSamePrecedence()
    {
        try
        {
            DiscoverableURIs discoverables =
                    new DiscoverableURIs.Builder()
                            .add( "a", "/test", DiscoverableURIs.NORMAL )
                            .add( "a", "/data", DiscoverableURIs.NORMAL )
                            .build();

            fail( "exception expected" );
        }
        catch ( Throwable t )
        {
            assertThat( t, is( instanceOf( InvalidSettingException.class ) ) );
            assertThat( t.getMessage(), startsWith( "Unable to add two entries with the same precedence using key " ) );
        }
    }

    @Test
    public void shouldInvokeConsumerForEachKeyWithHighestPrecedence() throws URISyntaxException
    {
        DiscoverableURIs discoverables =
                new DiscoverableURIs.Builder()
                        .add( "c", "bolt://localhost:7687", DiscoverableURIs.HIGHEST )
                        .add( "a", "/test", DiscoverableURIs.NORMAL )
                        .add( "b", "/data", DiscoverableURIs.NORMAL )
                        .add( "b", "/data2", DiscoverableURIs.LOWEST )
                        .add( "a", "/test2", DiscoverableURIs.HIGHEST )
                        .add( "c", "bolt://localhost:7688", DiscoverableURIs.LOW )
                        .build();

        discoverables.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", new URI( "/test2" ) );
        verify( consumer, times( 1 ) ).accept( "b", new URI( "/data" ) );
        verify( consumer, times( 1 ) ).accept( "c", new URI( "bolt://localhost:7687" ) );
    }

    @Test
    public void shouldInvokeConsumerForEachKeyWithHighestPrecedenceOnce() throws URISyntaxException
    {
        DiscoverableURIs discoverables =
                new DiscoverableURIs.Builder()
                        .add( "a", "/test1", DiscoverableURIs.LOWEST )
                        .add( "a", "/test2", DiscoverableURIs.LOW )
                        .add( "a", "/data3", DiscoverableURIs.NORMAL )
                        .add( "a", "/test4", DiscoverableURIs.HIGH )
                        .add( "a", "/test5", DiscoverableURIs.HIGHEST )
                        .build();

        discoverables.forEach( consumer );

        verify( consumer, only() ).accept( "a", new URI( "/test5" ) );
    }

    @Test
    public void shouldConvertStringIntoURI() throws URISyntaxException
    {
        DiscoverableURIs empty = new DiscoverableURIs.Builder()
                .add( "a", "bolt://localhost:7687", DiscoverableURIs.NORMAL )
                .build();

        empty.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", new URI( "bolt://localhost:7687" ) );
    }

    @Test
    public void shouldConvertSchemeHostPortIntoURI() throws URISyntaxException
    {
        DiscoverableURIs empty = new DiscoverableURIs.Builder()
                .add( "a", "bolt", "www.example.com", 8888, DiscoverableURIs.NORMAL )
                .build();

        empty.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", new URI( "bolt://www.example.com:8888" ) );
    }

    @Test
    public void shouldUsePassedURI() throws URISyntaxException
    {
        URI uri = new URI( "bolt://www.example.com:9999" );

        DiscoverableURIs empty = new DiscoverableURIs.Builder()
                .add( "a", uri, DiscoverableURIs.NORMAL )
                .build();

        empty.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", uri );
    }

    @Test
    public void shouldOverrideLowestForAbsolute() throws URISyntaxException
    {
        URI override = new URI( "http://www.example.com:9999" );
        DiscoverableURIs empty = new DiscoverableURIs.Builder()
                .add( "a", "bolt://localhost:8989", DiscoverableURIs.LOWEST )
                .overrideAbsolutesFromRequest( override )
                .build();

        empty.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", new URI( "bolt://www.example.com:8989" ) );
    }

    @Test
    public void shouldNotOverrideOtherThanLowestForAbsolute() throws URISyntaxException
    {
        URI override = new URI( "http://www.example.com:9999" );
        DiscoverableURIs empty = new DiscoverableURIs.Builder()
                .add( "a", "bolt://localhost:8989", DiscoverableURIs.LOW )
                .add( "b", "bolt://localhost:8990", DiscoverableURIs.NORMAL )
                .add( "c", "bolt://localhost:8991", DiscoverableURIs.HIGH )
                .add( "d", "bolt://localhost:8992", DiscoverableURIs.HIGHEST )
                .overrideAbsolutesFromRequest( override )
                .build();

        empty.forEach( consumer );

        verify( consumer, times( 1 ) ).accept( "a", new URI( "bolt://localhost:8989" ) );
        verify( consumer, times( 1 ) ).accept( "b", new URI( "bolt://localhost:8990" ) );
        verify( consumer, times( 1 ) ).accept( "c", new URI( "bolt://localhost:8991" ) );
        verify( consumer, times( 1 ) ).accept( "d", new URI( "bolt://localhost:8992" ) );
    }
}
