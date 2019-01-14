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
package org.neo4j.kernel.impl.store.id;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.validation.IdCapacityExceededException;
import org.neo4j.kernel.impl.store.id.validation.NegativeIdException;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IdGeneratorImplTest
{
    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final File file = new File( "ids" );

    @Test
    public void shouldNotAcceptMinusOne()
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, IdType.NODE, () -> 0L );

        expectedException.expect( NegativeIdException.class );

        // WHEN
        idGenerator.setHighId( -1 );
    }

    @Test
    public void throwsWhenNextIdIsTooHigh()
    {
        long maxId = 10;
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, maxId, false, IdType.NODE, () -> 0L );

        for ( long i = 0; i <= maxId; i++ )
        {
            idGenerator.nextId();
        }

        expectedException.expect( IdCapacityExceededException.class );
        expectedException.expectMessage( "Maximum id limit for NODE has been reached. Generated id 11 is out of " +
                "permitted range [0, 10]." );
        idGenerator.nextId();
    }

    @Test
    public void throwsWhenGivenHighIdIsTooHigh()
    {
        long maxId = 10;
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, maxId, false, IdType.RELATIONSHIP_TYPE_TOKEN, () -> 0L );

        expectedException.expect( IdCapacityExceededException.class );
        expectedException.expectMessage( "Maximum id limit for RELATIONSHIP_TYPE_TOKEN has been reached. Generated id 11 is out of permitted range [0, 10]." );
        idGenerator.setHighId( maxId + 1 );
    }

    /**
     * It should be fine to set high id to {@link IdGeneratorImpl#INTEGER_MINUS_ONE}.
     * It will just be never returned from {@link IdGeneratorImpl#nextId()}.
     */
    @Test
    public void highIdCouldBeSetToReservedId()
    {
        IdGeneratorImpl.createGenerator( fsr.get(), file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 1, Long.MAX_VALUE, false, IdType.NODE, () -> 0L );

        idGenerator.setHighId( IdGeneratorImpl.INTEGER_MINUS_ONE );

        assertEquals( IdGeneratorImpl.INTEGER_MINUS_ONE + 1, idGenerator.nextId() );
    }

    @Test
    public void correctDefragCountWhenHaveIdsInFile()
    {
        IdGeneratorImpl.createGenerator( fsr.get(), file, 100, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, true, IdType.NODE, () -> 100L );

        idGenerator.freeId( 5 );
        idGenerator.close();

        IdGenerator reloadedIdGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, true, IdType.NODE, () -> 100L );
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
    public void shouldReadDefragCountUsingStaticMethod() throws Exception
    {
        EphemeralFileSystemAbstraction fs = fsr.get();
        IdGeneratorImpl.createGenerator( fs, file, 0, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fs, file, 1, 10000, false, IdType.NODE, () -> 0L );
        idGenerator.nextId();
        long a = idGenerator.nextId();
        idGenerator.nextId();
        long b = idGenerator.nextId();
        idGenerator.nextId();
        idGenerator.freeId( a );
        idGenerator.freeId( b );
        long expectedDefragCount = idGenerator.getDefragCount();
        idGenerator.close();

        long actualDefragCount = IdGeneratorImpl.readDefragCount( fs, file );
        assertEquals( 2, expectedDefragCount );
        assertEquals( expectedDefragCount, actualDefragCount );
    }

    @Test
    public void shouldBeAbleToReadWrittenGenerator()
    {
        // Given
        IdGeneratorImpl.createGenerator( fsr.get(), file, 42, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, IdType.NODE, () -> 42L );

        idGenerator.close();

        // When
        idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, IdType.NODE, () -> 0L );

        // Then
        assertThat( idGenerator.getHighId(), equalTo( 42L ) );
    }

    @Test
    public void constructorShouldCallHighIdSupplierOnNonExistingIdFile()
    {
        // Given
        // An empty file (default, nothing to do)
        // and a mock supplier to test against
        LongSupplier highId = mock( LongSupplier.class );
        when( highId.getAsLong() ).thenReturn( 0L ); // necessary, otherwise it runs into NPE in the constructor below

        // Wheb
        // The id generator is started
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, IdType.NODE, highId );

        // Then
        // The highId supplier must have been called to get the high id
        verify( highId ).getAsLong();

        idGenerator.close();
    }

    @Test
    public void constructorShouldNotCallHighIdSupplierOnCleanIdFile()
    {
        // Given
        // A non empty, clean id file
        IdContainer.createEmptyIdFile( fsr.get(), file, 42, true );
        // and a mock supplier to test against
        LongSupplier highId = mock( LongSupplier.class );

        // When
        // An IdGenerator is created over the previous properly closed file
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, IdType.NODE, highId );
        idGenerator.close();

        // Then
        // The supplier must have remained untouched
        verifyZeroInteractions( highId );
    }
}
