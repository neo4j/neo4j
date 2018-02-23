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
package org.neo4j.server.enterprise;

import org.junit.jupiter.api.Test;

import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The classes that extend AbstractNeoServer are currently known to be:
 * CommunityNeoServer and EnterpriseNeoServer
 * <p>
 * This test asserts that those names won't change, for example during an
 * otherwise perfectly reasonable refactoring. Changing those names will cause
 * problems for the server which relies on those names to yield the correct
 * Neo4j edition (community, enterprise) to the Web UI and other clients.
 * <p>
 * Although this test asserts naming against classes in other modules (neo4j),
 * it lives in neo4j-enterprise because otherwise the CommunityNeoServer
 * and EnterpriseNeoServer would not be visible.
 */
public class ServerClassNameTest
{
    @Test
    public void shouldMaintainNamingOfCommunityNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
    {
        assertEquals( "communityneoserver", CommunityNeoServer.class.getSimpleName().toLowerCase(),
                getErrorMessage( CommunityNeoServer.class ) );
    }

    @Test
    public void shouldMaintainNamingOfEnterpriseNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
    {
        assertEquals( "openenterpriseneoserver", OpenEnterpriseNeoServer.class.getSimpleName().toLowerCase(),
                getErrorMessage( OpenEnterpriseNeoServer.class ) );
    }

    private String getErrorMessage( Class<? extends AbstractNeoServer> neoServerClass )
    {
        return "The " + neoServerClass.getSimpleName() + " class appears to have been renamed. There is a strict " +
                "dependency from the REST API VersionAndEditionService on the name of that class. If you want " +
                "to change the name of that class, then remember to change VersionAndEditionService, " +
                "VersionAndEditionServiceTest and, of course, this test. ";
    }
}
