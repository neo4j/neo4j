/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest;

import org.junit.Before;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.web.JsonAndHtmlWebService;

import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexNodeFunctionalityServiceTest extends IndexNodeFunctionalityBase
{
    @Before
    public void init() throws IOException
    {

        new JsonAndHtmlWebService( uriInfo(), new Database(new ImpermanentGraphDatabase()));
    }

    private UriInfo uriInfo()
    {
        UriInfo mockUriInfo = mock( UriInfo.class );
        try
        {
            when( mockUriInfo.getBaseUri() ).thenReturn( new URI( "http://neo4j.org/" ) );
        } catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }

        return mockUriInfo;
    }

    @Override
    TestResponse getIndex_Node()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
