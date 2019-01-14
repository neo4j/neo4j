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
package org.neo4j.server.enterprise;

import org.junit.Test;

import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;

import static org.junit.Assert.assertEquals;

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
        assertEquals( getErrorMessage( CommunityNeoServer.class ), "communityneoserver",
                CommunityNeoServer.class.getSimpleName().toLowerCase() );
    }

    @Test
    public void shouldMaintainNamingOfEnterpriseNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
    {
        assertEquals( getErrorMessage( OpenEnterpriseNeoServer.class ), "openenterpriseneoserver",
                OpenEnterpriseNeoServer.class.getSimpleName().toLowerCase() );
    }

    private String getErrorMessage( Class<? extends AbstractNeoServer> neoServerClass )
    {
        return "The " + neoServerClass.getSimpleName() + " class appears to have been renamed. There is a strict " +
                "dependency from the REST API VersionAndEditionService on the name of that class. If you want " +
                "to change the name of that class, then remember to change VersionAndEditionService, " +
                "VersionAndEditionServiceTest and, of course, this test. ";
    }
}
