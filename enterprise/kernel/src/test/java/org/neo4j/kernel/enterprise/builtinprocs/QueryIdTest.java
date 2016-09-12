/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.fromExternalString;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;

public class QueryIdTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void printsQueryIds() throws InvalidArgumentsException
    {
        assertThat( ofInternalId( 12L ).toString(), equalTo( "query-12" ) );
    }

    @Test
    public void doesNotConstructNegativeQueryIds() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        ofInternalId( -15L );
    }

    @Test
    public void parsesQueryIds() throws InvalidArgumentsException
    {
        assertThat( fromExternalString( "query-14" ), equalTo( ofInternalId( 14L ) ) );
    }

    @Test
    public void doesNotParseNegativeQueryIds() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "query--12" );
    }

    @Test
    public void doesNotParseRandomText() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "blarglbarf" );
    }

    @Test
    public void doesNotParseTrailingRandomText() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "query-12  " );
    }

    @Test
    public void doesNotParseEmptyText() throws InvalidArgumentsException
    {
        thrown.expect( InvalidArgumentsException.class );
        fromExternalString( "" );
    }
}
