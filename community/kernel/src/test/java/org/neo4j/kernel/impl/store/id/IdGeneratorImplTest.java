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
package org.neo4j.kernel.impl.store.id;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.fail;

public class IdGeneratorImplTest
{
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

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    private final File file = new File( "ids" );
}
