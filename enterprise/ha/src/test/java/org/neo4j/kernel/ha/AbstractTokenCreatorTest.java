/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
