/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.rest;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class EnterpriseDiscoverableURIsTest
{
    @Test
    public void shouldExposeBoltRoutingIfCore() throws Exception
    {
        // Given
        BoltConnector bolt = new BoltConnector( "honestJakesBoltConnector" );
        Config config = Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, EnterpriseEditionSettings.Mode.CORE.name() )
                .withSetting( bolt.enabled, "true" )
                .withSetting( bolt.type, BoltConnector.ConnectorType.BOLT.name() )
                .build();

        // When
        Map<String,Object> asd = toMap(
                EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, new ConnectorPortRegister() ) );

        // Then
        assertThat(asd.get("bolt_routing"), equalTo( "bolt+routing://localhost:7687" ));
    }

    @Test
    public void shouldGrabPortFromRegisterIfSetTo0() throws Exception
    {
        // Given
        BoltConnector bolt = new BoltConnector( "honestJakesBoltConnector" );
        Config config = Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, EnterpriseEditionSettings.Mode.CORE.name() )
                .withSetting( bolt.enabled, "true" )
                .withSetting( bolt.type, BoltConnector.ConnectorType.BOLT.name() )
                .withSetting( bolt.listen_address, ":0" )
                .build();
        ConnectorPortRegister ports = new ConnectorPortRegister();
        ports.register( bolt.key(), new InetSocketAddress( 1337 ) );

        // When
        Map<String,Object> asd = toMap(
                EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs( config, ports ) );

        // Then
        assertThat(asd.get("bolt_routing"), equalTo( "bolt+routing://localhost:1337" ));
    }

    private Map<String,Object> toMap( DiscoverableURIs uris )
    {
        Map<String,Object> out = new HashMap<>();
        uris.forEachAbsoluteUri( ( k, v ) -> out.put( k, v.toASCIIString() ) );
        uris.forEachRelativeUri( out::put );
        return out;
    }
}
