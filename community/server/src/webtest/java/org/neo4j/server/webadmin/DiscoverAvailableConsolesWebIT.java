/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.webadmin;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;

import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;

import static org.neo4j.server.helpers.CommunityServerBuilder.server;

@Ignore("On avengers wall to be resolved")
public class DiscoverAvailableConsolesWebIT extends AbstractExclusiveServerWebadminTest
{

    @Test
    public void shouldShowOnlyShellIfAvailable() throws Exception
    {
        NeoServer server = server().build();
        try
        {
            server.start();
            setupWebdriver( server );

            wl.goToWebadminStartPage();
            wl.clickOnTab( "Console" );
            wl.waitForElementToDisappear( By
                    .xpath( "//div[@id='console-tabs']//a[contains(.,'Gremlin')]" ) );
            wl.waitForElementToAppear( By
                    .xpath( "//div[@id='console-tabs']//a[contains(.,'Neo4j Shell')]" ) );

        }
        finally
        {
            shutdownWebdriver();
            server.stop();
        }
    }

    @Test
    public void shouldNotShowGremlinIfNotAvailable() throws Exception
    {
        NeoServer server = server().withProperty( Configurator.MANAGEMENT_CONSOLE_ENGINES, "shell" ).build();
        try
        {
            server.start();
            setupWebdriver( server );

            wl.goToWebadminStartPage();
            wl.clickOnTab( "Console" );
            wl.waitForElementToDisappear( By
                    .xpath( "//div[@id='console-tabs']//a[contains(.,'Gremlin')]" ) );
            wl.waitForElementToAppear( By
                    .xpath( "//div[@id='console-tabs']//a[contains(.,'Neo4j Shell')]" ) );

        }
        finally
        {
            shutdownWebdriver();
            server.stop();
        }
    }

    @Test
    public void shouldNotShowEitherShellIfBothAreDisabled() throws Exception
    {
        NeoServer server = server().withProperty( Configurator.MANAGEMENT_CONSOLE_ENGINES, "" ).build();
        try
        {
            server.start();
            setupWebdriver( server );

            wl.goToWebadminStartPage();
            wl.clickOnTab( "Console" );
            wl.waitForElementToDisappear( By
                    .xpath( "//div[@id='console-tabs']//a[contains(.,'Gremlin')]" ) );
            wl.waitForElementToDisappear( By
                    .xpath( "//div[@id='console-tabs']//a[contains(.,'Neo4j Shell')]" ) );

        }
        finally
        {
            shutdownWebdriver();
            server.stop();
        }
    }

}
