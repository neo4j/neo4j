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
package org.neo4j.io.pagecache;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.impl.common.SingleFilePageSwapperFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public abstract class PageSwappingTest
{
    private static PageEvictionCallback NO_CALLBACK = new PageEvictionCallback()
    {
        @Override
        public void onEvict( long pageId )
        {
        }
    };

    private static EphemeralFileSystemAbstraction fs;

    private final PageSwapperFactory swapperFactory;

    @BeforeClass
    public static void setUp()
    {
        Thread.interrupted(); // Clear stray interrupts
        fs = new EphemeralFileSystemAbstraction();
    }

    @AfterClass
    public static void tearDown()
    {
        fs.shutdown();
    }

    @Parameterized.Parameters
    public static Iterable<Object[]> dataPoints()
    {
        Factory<PageSwapperFactory> singleFileSwapper = new Factory<PageSwapperFactory>()
        {
            @Override
            public PageSwapperFactory newInstance()
            {
                return new SingleFilePageSwapperFactory( fs );
            }
        };

        return Arrays.asList( new Object[][]{
                { singleFileSwapper }
                } );
    }

    public PageSwappingTest( Factory<PageSwapperFactory> fixture )
    {
        swapperFactory = fixture.newInstance();
    }

    protected abstract Page createPage( int cachePageSize );

    protected abstract long writeLock( Page page );

    protected abstract void unlockWrite( Page page, long stamp );

    @Before
    @After
    public void clearStrayInterrupts()
    {
        Thread.interrupted();
    }

    @Test
    public void swappingOutMustEitherThrowOrNotSwallowInterrupts() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        Page page = createPage( 20 );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, 20, NO_CALLBACK );

        Thread.currentThread().interrupt();

        long stamp = writeLock( page );
        try
        {
            swapper.write( 0, page );
            System.out.println("poke");
            assertTrue( Thread.currentThread().isInterrupted() );
        }
        catch ( ClosedByInterruptException exception )
        {
            assertThat( exception, instanceOf( ClosedByInterruptException.class ) );

            // Unlike with InterruptedException, the ClosedByInterruptException
            // does not imply clearing the interrupted status flag
            assertTrue( Thread.currentThread().isInterrupted() );
        }
        finally
        {
            unlockWrite( page, stamp );
        }
    }
}
