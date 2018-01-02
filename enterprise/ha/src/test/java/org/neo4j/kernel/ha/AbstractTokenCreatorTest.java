/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.com.ResourceReleaser.NO_OP;

public class AbstractTokenCreatorTest
{
    private final Master master = mock( Master.class );
    private final RequestContextFactory requestContextFactory = mock( RequestContextFactory.class );

    private final RequestContext context = new RequestContext( 1, 2, 3, 4, 5 );

    private final String label = "A";
    private final Response<Integer> response = new TransactionStreamResponse<>( 42, null, null, NO_OP );

    private final AbstractTokenCreator creator = new AbstractTokenCreator( master, requestContextFactory )
    {
        @Override
        protected Response<Integer> create( Master master, RequestContext context, String name )
        {
            assertEquals( AbstractTokenCreatorTest.this.master, master );
            assertEquals( AbstractTokenCreatorTest.this.context, context );
            assertEquals( AbstractTokenCreatorTest.this.label, name );
            return response;
        }
    };

    @Before
    public void setup()
    {
        when( requestContextFactory.newRequestContext() ).thenReturn( context );
    }

    @Test
    public void shouldCreateALabelOnMasterAndApplyItLocally()
    {
        // GIVEN
        int responseValue = response.response();

        // WHEN
        int result = creator.getOrCreate( label );

        // THEN
        assertEquals( responseValue, result );
    }

    @Test
    public void shouldThrowIfCreateThrowsAnException()
    {
        // GIVEN
        RuntimeException re = new RuntimeException( "IO" );
        AbstractTokenCreator throwingCreator = spy( creator );
        doThrow( re ).when( throwingCreator ).create( any( Master.class ), any( RequestContext.class ), anyString() );

        try
        {
            // WHEN
            throwingCreator.getOrCreate( "A" );
            fail( "Should have thrown" );
        }
        catch ( Exception e )
        {
            // THEN
            assertEquals( re, e );
        }
    }
}
