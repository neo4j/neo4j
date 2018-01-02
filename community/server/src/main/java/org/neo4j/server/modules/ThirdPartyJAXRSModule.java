/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.modules;

import java.util.Collection;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.web.WebServer;

import static java.util.Arrays.asList;

public class ThirdPartyJAXRSModule implements ServerModule
{
	private final Config config;
	private final WebServer webServer;

    private final ExtensionInitializer extensionInitializer;
	private List<ThirdPartyJaxRsPackage> packages;
    private final Log log;

    public ThirdPartyJAXRSModule( WebServer webServer, Config config, LogProvider logProvider,
            NeoServer neoServer )
    {
    	this.webServer = webServer;
    	this.config = config;
    	this.log = logProvider.getLog( getClass() );
        extensionInitializer = new ExtensionInitializer( neoServer );
    }

    @Override
	public void start()
    {
        this.packages = config.get( ServerSettings.third_party_packages );
        for ( ThirdPartyJaxRsPackage tpp : packages )
        {
            List<String> packageNames = packagesFor( tpp );
            Collection<Injectable<?>> injectables = extensionInitializer.initializePackages( packageNames );
            webServer.addJAXRSPackages( packageNames, tpp.getMountPoint(), injectables );
            log.info( "Mounted unmanaged extension [%s] at [%s]", tpp.getPackageName(), tpp.getMountPoint() );
        }
    }

    private List<String> packagesFor( ThirdPartyJaxRsPackage tpp )
    {
        return asList( tpp.getPackageName() );
    }

    @Override
	public void stop()
    {
        if ( packages == null )
        {
            return;
        }

        for ( ThirdPartyJaxRsPackage tpp : packages )
        {
            webServer.removeJAXRSPackages( packagesFor( tpp ), tpp.getMountPoint() );
        }

        extensionInitializer.stop();
    }
}
