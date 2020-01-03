/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class TokenTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void labelGetOrCreateForName()
    {
        assertIllegalToken( token -> token.labelGetOrCreateForName( null ) );
        assertIllegalToken( token -> token.labelGetOrCreateForName( "" ) );
        int id = mapToken( token -> token.labelGetOrCreateForName( "label" ) );
        assertEquals( id, mapToken( token -> token.nodeLabel( "label" ) ) );
    }

    @Test
    public void labelGetOrCreateForNames()
    {
        assertIllegalToken( token -> token.labelGetOrCreateForNames( new String[]{null}, new int[1] ) );
        assertIllegalToken( token -> token.labelGetOrCreateForNames( new String[]{""}, new int[1] ) );
        String[] names = {"a", "b"};
        int[] ids = new int[2];
        forToken( token -> token.labelGetOrCreateForNames( names, ids ) );
        assertEquals( ids[0], mapToken( token -> token.nodeLabel( "a" ) ) );
        assertEquals( ids[1], mapToken( token -> token.nodeLabel( "b" ) ) );
    }

    @Test
    public void propertyKeyGetOrCreateForName()
    {
        assertIllegalToken( token -> token.propertyKeyGetOrCreateForName( null ) );
        assertIllegalToken( token -> token.propertyKeyGetOrCreateForName( "" ) );
        int id = mapToken( token -> token.propertyKeyGetOrCreateForName( "prop" ) );
        assertEquals( id, mapToken( token -> token.propertyKey( "prop" ) ) );
    }

    @Test
    public void propertyKeyGetOrCreateForNames()
    {
        assertIllegalToken( token -> token.propertyKeyGetOrCreateForNames( new String[]{null}, new int[1] ) );
        assertIllegalToken( token -> token.propertyKeyGetOrCreateForNames( new String[]{""}, new int[1] ) );
        String[] names = {"a", "b"};
        int[] ids = new int[2];
        forToken( token -> token.propertyKeyGetOrCreateForNames( names, ids ) );
        assertEquals( ids[0], mapToken( token -> token.propertyKey( "a" ) ) );
        assertEquals( ids[1], mapToken( token -> token.propertyKey( "b" ) ) );
    }

    @Test
    public void relationshipTypeGetOrCreateForName()
    {
        assertIllegalToken( token -> token.relationshipTypeGetOrCreateForName( null ) );
        assertIllegalToken( token -> token.relationshipTypeGetOrCreateForName( "" ) );
        int id = mapToken( token -> token.relationshipTypeGetOrCreateForName( "rel" ) );
        assertEquals( id, mapToken( token -> token.relationshipType( "rel" ) ) );
    }

    @Test
    public void relationshipTypeGetOrCreateForNames()
    {
        assertIllegalToken( token -> token.relationshipTypeGetOrCreateForNames( new String[]{null}, new int[1] ) );
        assertIllegalToken( token -> token.relationshipTypeGetOrCreateForNames( new String[]{""}, new int[1] ) );
        String[] names = {"a", "b"};
        int[] ids = new int[2];
        forToken( token -> token.relationshipTypeGetOrCreateForNames( names, ids ) );
        assertEquals( ids[0], mapToken( token -> token.relationshipType( "a" ) ) );
        assertEquals( ids[1], mapToken( token -> token.relationshipType( "b" ) ) );
    }

    private void assertIllegalToken( ThrowingConsumer<Token, KernelException> f )
    {
        try ( Transaction tx = beginTransaction() )
        {
            f.accept( tx.token() );
            fail( "Expected IllegalTokenNameException" );
        }
        catch ( IllegalTokenNameException e )
        {
            // wanted
        }
        catch ( KernelException e )
        {
            fail( "Unwanted exception: " + e.getMessage() );
        }
    }

    private int mapToken( ThrowingFunction<Token, Integer, KernelException> f )
    {
        try ( Transaction tx = beginTransaction() )
        {
            return f.apply( tx.token() );
        }
        catch ( KernelException e )
        {
            fail( "Unwanted exception: " + e.getMessage() );
            return -1; // unreachable
        }
    }

    private void forToken( ThrowingConsumer<Token, KernelException> f )
    {
        try ( Transaction tx = beginTransaction() )
        {
            f.accept( tx.token() );
        }
        catch ( KernelException e )
        {
            fail( "Unwanted exception: " + e.getMessage() );
        }
    }
}
