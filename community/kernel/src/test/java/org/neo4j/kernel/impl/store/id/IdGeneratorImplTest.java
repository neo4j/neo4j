/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class IdGeneratorImplTest
{

    public final
    @Rule
    EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final File file = new File( "ids" );

    @Test
    public void shouldNotAcceptMinusOne() throws Exception
    {
        // GIVEN
        IdGeneratorImpl.createGenerator( fsr.get(), file );
        IdGenerator idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 0 );

        // WHEN
        try
        {
            idGenerator.setHighId( -1 );
            fail( "Should have failed" );
        }
        catch ( UnderlyingStorageException e )
        {   // OK
        }
    }

    @Test
    public void shouldReadHighIdUsingStaticMethod() throws Exception
    {
        // GIVEN
        long highId = 12345L;
        IdGeneratorImpl.createGenerator( fsr.get(), file, highId );

        // WHEN
        long readHighId = IdGeneratorImpl.readHighId( fsr.get(), file );

        // THEN
        assertEquals( highId, readHighId );
    }

    @Test
    public void shouldBeAbleToReadWrittenGenerator()
    {
        // Given
        IdGeneratorImpl.createGenerator( fsr.get(), file );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 42 );

        idGenerator.close();

        // When
        idGenerator = new IdGeneratorImpl( fsr.get(), file, 100, 100, false, 0 );

        // Then
        assertThat( idGenerator.getHighId(), equalTo( 42L ) );
    }
}
