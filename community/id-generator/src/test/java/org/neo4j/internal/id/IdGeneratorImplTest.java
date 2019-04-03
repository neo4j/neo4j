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
package org.neo4j.internal.id;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.id.IdGeneratorImpl.INTEGER_MINUS_ONE;

@ExtendWith( EphemeralFileSystemExtension.class )
class IdGeneratorImplTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private final File file = new File( "ids" );

    @BeforeEach
    void setUp()
    {
        fs.clear();
    }

    @Test
    void shouldNotAcceptMinusOne()
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fs, file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, file, 100, 100, false, IdType.NODE, () -> 0L );

        assertThrows( NegativeIdException.class, () -> idGenerator.setHighId( -1 ) );
    }

    @Test
    void throwsWhenNextIdIsTooHigh()
    {
        long maxId = 10;
        IdGeneratorImpl.createGenerator( fs, file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, file, 1, maxId, false, IdType.NODE, () -> 0L );

        for ( long i = 0; i <= maxId; i++ )
        {
            idGenerator.nextId();
        }

        IdCapacityExceededException exception = assertThrows( IdCapacityExceededException.class, idGenerator::nextId );
        assertThat( exception.getMessage(),
                containsString( "Maximum id limit for NODE has been reached. Generated id 11 is out of permitted range [0, 10]." ) );
    }

    @Test
    void throwsWhenGivenHighIdIsTooHigh()
    {
        long maxId = 10;
        IdGeneratorImpl.createGenerator( fs, file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, file, 1, maxId, false, IdType.RELATIONSHIP_TYPE_TOKEN, () -> 0L );

        IdCapacityExceededException exception = assertThrows( IdCapacityExceededException.class, () -> idGenerator.setHighId( maxId + 1 ) );
        assertThat( exception.getMessage(),
                containsString( "Maximum id limit for RELATIONSHIP_TYPE_TOKEN has been reached. Generated id 11 is out of permitted range [0, 10]." ) );
    }

    /**
     * It should be fine to set high id to {@link IdGeneratorImpl#INTEGER_MINUS_ONE}.
     * It will just be never returned from {@link IdGeneratorImpl#nextId()}.
     */
    @Test
    void highIdCouldBeSetToReservedId()
    {
        IdGeneratorImpl.createGenerator( fs, file, 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, file, 1, Long.MAX_VALUE, false, IdType.NODE, () -> 0L );

        idGenerator.setHighId( INTEGER_MINUS_ONE );

        assertEquals( INTEGER_MINUS_ONE + 1, idGenerator.nextId() );
    }

    @Test
    void correctDefragCountWhenHaveIdsInFile()
    {
        IdGeneratorImpl.createGenerator( fs, file, 100, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, file, 100, 100, true, IdType.NODE, () -> 100L );

        idGenerator.freeId( 5 );
        idGenerator.close();

        IdGenerator reloadedIdGenerator = new IdGeneratorImpl( fs, file, 100, 100, true, IdType.NODE, () -> 100L );
        assertEquals( 1, reloadedIdGenerator.getDefragCount() );
        assertEquals( 5, reloadedIdGenerator.nextId() );
        assertEquals( 0, reloadedIdGenerator.getDefragCount() );
    }

    @Test
    void shouldReadHighIdUsingStaticMethod() throws Exception
    {
        // GIVEN
        long highId = 12345L;
        IdGeneratorImpl.createGenerator( fs, file, highId, false );

        // WHEN
        long readHighId = IdGeneratorImpl.readHighId( fs, file );

        // THEN
        assertEquals( highId, readHighId );
    }

    @Test
    void shouldReadDefragCountUsingStaticMethod() throws Exception
    {
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
    void shouldBeAbleToReadWrittenGenerator()
    {
        // Given
        IdGeneratorImpl.createGenerator( fs, file, 42, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fs, file, 100, 100, false, IdType.NODE, () -> 42L );

        idGenerator.close();

        // When
        idGenerator = new IdGeneratorImpl( fs, file, 100, 100, false, IdType.NODE, () -> 0L );

        // Then
        assertThat( idGenerator.getHighId(), equalTo( 42L ) );
    }

    @Test
    void constructorShouldCallHighIdSupplierOnNonExistingIdFile()
    {
        // Given
        // An empty file (default, nothing to do)
        // and a mock supplier to test against
        LongSupplier highId = mock( LongSupplier.class );
        when( highId.getAsLong() ).thenReturn( 0L ); // necessary, otherwise it runs into NPE in the constructor below

        // When
        // The id generator is started
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fs, file, 100, 100, false, IdType.NODE, highId );

        // Then
        // The highId supplier must have been called to get the high id
        verify( highId ).getAsLong();

        idGenerator.close();
    }

    @Test
    void constructorShouldNotCallHighIdSupplierOnCleanIdFile()
    {
        // Given
        // A non empty, clean id file
        IdContainer.createEmptyIdFile( fs, file, 42, true );
        // and a mock supplier to test against
        LongSupplier highId = mock( LongSupplier.class );

        // When
        // An IdGenerator is created over the previous properly closed file
        IdGenerator idGenerator = new IdGeneratorImpl( fs, file, 100, 100, false, IdType.NODE, highId );
        idGenerator.close();

        // Then
        // The supplier must have remained untouched
        verifyZeroInteractions( highId );
    }
}
