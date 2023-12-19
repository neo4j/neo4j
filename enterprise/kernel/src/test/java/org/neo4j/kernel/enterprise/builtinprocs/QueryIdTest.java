/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
