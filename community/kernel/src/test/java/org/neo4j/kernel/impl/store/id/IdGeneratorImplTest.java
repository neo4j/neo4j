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
package org.neo4j.kernel.impl.store.id;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.InvalidIdGeneratorException;
import org.neo4j.kernel.impl.store.id.validation.IdCapacityExceededException;
import org.neo4j.kernel.impl.store.id.validation.NegativeIdException;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.test.ProcessTestUtil.executeSubProcess;

public class IdGeneratorImplTest
{
    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final File file = new File( "ids" );

    @Test
    public void shouldNotAcceptMinusOne() throws Exception
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 0 );

        // WHEN
        try
        {
            idGenerator.setHighId( -1 );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NegativeIdException.class ) );
        }
    }

    @Test
    public void throwsWhenNextIdIsTooHigh()
    {
        long maxId = 10;
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, maxId, false, 0 );

        for ( long i = 0; i <= maxId; i++ )
        {
            idGenerator.nextId();
        }

        try
        {
            idGenerator.nextId();
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IdCapacityExceededException.class ) );
        }
    }

    @Test
    public void throwsWhenGivenHighIdIsTooHigh()
    {
        long maxId = 10;
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, maxId, false, 0 );

        try
        {
            idGenerator.setHighId( maxId + 1 );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IdCapacityExceededException.class ) );
        }
    }

    /**
     * It should be fine to set high id to {@link IdGeneratorImpl#INTEGER_MINUS_ONE}.
     * It will just be never returned from {@link IdGeneratorImpl#nextId()}.
     */
    @Test
    public void highIdCouldBeSetToReservedId()
    {
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, Long.MAX_VALUE, false, 0 );

        idGenerator.setHighId( IdGeneratorImpl.INTEGER_MINUS_ONE );

        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idGenerator.nextId() );
    }

    @Test
    public void correctDefragCountWhenHaveIdsInFile()
    {
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, true, 100 );

        idGenerator.freeId( 5 );
        idGenerator.close();

        IdGenerator reloadedIdGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, true, 100 );
        assertEquals( 1, reloadedIdGenerator.getDefragCount() );
        assertEquals( 5, reloadedIdGenerator.nextId() );
        assertEquals( 0, reloadedIdGenerator.getDefragCount() );
    }

    @Test
    public void shouldReadHighIdUsingStaticMethod() throws Exception
    {
        // GIVEN
        long highId = 12345L;
        IdGeneratorImpl.createGenerator( fsr.get(), file, highId, false );

        // WHEN
        long readHighId = IdGeneratorImpl.readHighId( fsr.get(), file );

        // THEN
        assertEquals( highId, readHighId );
    }

    @Test
    public void shouldBeAbleToReadWrittenGenerator()
    {
        // Given
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 42 );

        idGenerator.close();

        // When
        idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 0 );

        // Then
        assertThat( idGenerator.getHighId(), equalTo( 42L ) );
    }

    @Test
    public void shouldForceStickyMark() throws Exception
    {
        // GIVEN
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction() )
        {
            File dir = new File( "target/test-data/" + getClass().getName() );
            fs.mkdirs( dir );
            File file = new File( dir, "ids" );
            fs.deleteFile( file );
            IdGeneratorImpl.createGenerator( fs, file, 0, false );

            // WHEN opening the id generator, where the jvm crashes right after
            executeSubProcess( getClass(), 1, MINUTES, file.getAbsolutePath() );

            // THEN
            try
            {
                IdGeneratorImpl.readHighId( fs, file );
                fail( "Should have thrown, saying something with sticky generator" );
            }
            catch ( InvalidIdGeneratorException e )
            {
                // THEN Good
            }
        }
    }

    @Test
    public void shouldDeleteIfOpen() throws Exception
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 42, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 42 );

        // WHEN
        idGenerator.delete();

        // THEN
        assertFalse( fsr.get().fileExists( file ) );
    }

    @Test
    public void shouldDeleteIfClosed() throws Exception
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 42, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 42 );
        idGenerator.close();

        // WHEN
        idGenerator.delete();

        // THEN
        assertFalse( fsr.get().fileExists( file ) );
    }

    @Test
    public void shouldTruncateTheFileIfOverwriting() throws Exception
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 10, true );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 5, 100, false, 30 );
        for ( int i = 0; i < 17; i++ )
        {
            idGenerator.freeId( i );
        }
        idGenerator.close();
        assertThat( (int) fsr.get().getFileSize( file ), greaterThan( IdGeneratorImpl.HEADER_SIZE ) );

        // WHEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 30, false );

        // THEN
        assertEquals( IdGeneratorImpl.HEADER_SIZE, (int) fsr.get().getFileSize( file ) );
        assertEquals( 30, IdGeneratorImpl.readHighId( fsr.get(), file ) );
        idGenerator = new IdGeneratorImpl( fsr.get(), file, 5, 100, false, 30 );
        assertEquals( 30, idGenerator.nextId() );
    }

    public static void main( String[] args ) throws IOException
    {
        // Leave it opened
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            new IdGeneratorImpl( fileSystem, new File( args[0] ), 100, 100, false, 42 );
        }
        System.exit( 0 );
    }
}
