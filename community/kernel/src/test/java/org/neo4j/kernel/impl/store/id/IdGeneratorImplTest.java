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

import java.io.File;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.store.id.validation.IdCapacityExceededException;
import org.neo4j.kernel.impl.store.id.validation.NegativeIdException;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IdGeneratorImplTest
{
    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final File file = new File( "ids" );

    @Test
    public void shouldNotAcceptMinusOne() throws Exception
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, () -> 0L );

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
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, maxId, false, () -> 0L );

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
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, maxId, false, () -> 0L );

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
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, Long.MAX_VALUE, false, () -> 0L );

        idGenerator.setHighId( IdGeneratorImpl.INTEGER_MINUS_ONE );

        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idGenerator.nextId() );
    }

    @Test
    public void correctDefragCountWhenHaveIdsInFile()
    {
        IdGeneratorImpl.createGenerator( fsr.get(), file, 100, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, true, () -> 100L );

        idGenerator.freeId( 5 );
        idGenerator.close();

        IdGenerator reloadedIdGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, true, () -> 100L );
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
        IdGeneratorImpl.createGenerator( fsr.get(), file, 42, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, () -> 42L );

        idGenerator.close();

        // When
        idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, () -> 0L );

        // Then
        assertThat( idGenerator.getHighId(), equalTo( 42L ) );
    }

    @Test
    public void constructorShouldCallHighIdSupplierOnNonExistingIdFile() throws Exception
    {
        // Given
        // An empty file (default, nothing to do)
        // and a mock supplier to test against
        Supplier<Long> highId = mock( Supplier.class );
        when( highId.get() ).thenReturn( 0L ); // necessary, otherwise it runs into NPE in the constructor below

        // Wheb
        // The id generator is started
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, highId );

        // Then
        // The highId supplier must have been called to get the high id
        verify( highId ).get();

        idGenerator.close();
    }

    @Test
    public void constructorShouldNotCallHighIdSupplierOnCleanIdFile() throws Exception
    {
        // Given
        // A non empty, clean id file
        IdContainer.createEmptyIdFile( fsr.get(), file, 42, true );
        // and a mock supplier to test against
        Supplier<Long> highId = mock( Supplier.class );

        // When
        // An IdGenerator is created over the previous properly closed file
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, highId );
        idGenerator.close();

        // Then
        // The supplier must have remained untouched
        verifyZeroInteractions( highId );
    }
}
