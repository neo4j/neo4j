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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.test.ConstantRequestContextFactory;
import org.neo4j.test.IntegerResponse;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( Parameterized.class )
public class SlaveTokenCreatorTest
{
    public interface SlaveTokenCreatorFixture
    {
        AbstractTokenCreator build( Master master, RequestContextFactory requestContextFactory );
        Response<Integer> callMasterMethod( Master master, RequestContext ctx, String name );
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> tokenCreators()
    {
        SlaveTokenCreatorFixture slaveLabelTokenCreatorFixture = new SlaveTokenCreatorFixture()
        {
            @Override
            public AbstractTokenCreator build( Master master, RequestContextFactory requestContextFactory )
            {
                return new SlaveLabelTokenCreator( master, requestContextFactory );
            }

            @Override
            public Response<Integer> callMasterMethod( Master master, RequestContext ctx, String name )
            {
                return master.createLabel( ctx, name );
            }
        };
        SlaveTokenCreatorFixture slaveRelationshipTypeTokenCreatorFixture = new SlaveTokenCreatorFixture()
        {
            @Override
            public AbstractTokenCreator build( Master master, RequestContextFactory requestContextFactory )
            {
                return new SlaveRelationshipTypeCreator( master, requestContextFactory );
            }

            @Override
            public Response<Integer> callMasterMethod( Master master, RequestContext ctx, String name )
            {
                return master.createRelationshipType( ctx, name );
            }
        };
        SlaveTokenCreatorFixture slavePropertyTokenCreatorFixture = new SlaveTokenCreatorFixture()
        {
            @Override
            public AbstractTokenCreator build( Master master, RequestContextFactory requestContextFactory )
            {
                return new SlavePropertyTokenCreator( master, requestContextFactory );
            }

            @Override
            public Response<Integer> callMasterMethod( Master master, RequestContext ctx, String name )
            {
                return master.createPropertyKey( ctx, name );
            }
        };
        return Arrays.asList(
                new Object[] {"SlaveLabelTokenCreator", slaveLabelTokenCreatorFixture},
                new Object[] {"SlaveRelationshipTypeTokenCreator", slaveRelationshipTypeTokenCreatorFixture},
                new Object[] {"SlavePropertyTokenCreator", slavePropertyTokenCreatorFixture}
        );
    }

    private SlaveTokenCreatorFixture fixture;
    private Master master;
    private RequestContext requestContext;
    private RequestContextFactory requestContextFactory;
    private String name;
    private AbstractTokenCreator tokenCreator;

    public SlaveTokenCreatorTest( String name, SlaveTokenCreatorFixture fixture )
    {
        this.fixture = fixture;
        master = mock( Master.class );
        requestContext = new RequestContext( 1, 2, 3, 4, 5 );
        this.name = "Poke";
        requestContextFactory = new ConstantRequestContextFactory( requestContext );
        tokenCreator = fixture.build( master, requestContextFactory );
    }

    @Test( expected = TransientTransactionFailureException.class )
    public void mustTranslateComExceptionsToTransientTransactionFailures() throws Exception
    {
        when( fixture.callMasterMethod( master, requestContext, name ) ).thenThrow( new ComException() );
        tokenCreator.getOrCreate( name );
    }

    @Test
    public void mustReturnIdentifierFromMaster() throws Exception
    {
        when( fixture.callMasterMethod( master, requestContext, name ) ).thenReturn( new IntegerResponse( 13 ) );
        assertThat( tokenCreator.getOrCreate( name ), is( 13 ) );
    }
}
