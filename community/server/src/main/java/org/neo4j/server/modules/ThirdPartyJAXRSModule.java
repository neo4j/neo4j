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
package org.neo4j.server.modules;

import static org.neo4j.server.JAXRSHelper.listFrom;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.web.WebServer;

public class ThirdPartyJAXRSModule implements ServerModule
{
    private final Logger log = Logger.getLogger( ThirdPartyJAXRSModule.class );

	private final Configurator configurator;
	private final WebServer webServer;

    private ExtensionInitializer extensionInitializer;
	private Set<ThirdPartyJaxRsPackage> packages;


    public ThirdPartyJAXRSModule( WebServer webServer, Configurator configurator, NeoServer neoServer )
    {
    	this.webServer = webServer;
    	this.configurator = configurator;
        extensionInitializer = new ExtensionInitializer( neoServer );
    }

    @Override
	public void start(StringLogger logger)
    {
        this.packages = configurator.getThirdpartyJaxRsPackages();
        for ( ThirdPartyJaxRsPackage tpp : packages )
        {
            List<String> packageNames = packagesFor( tpp );
            Collection<Injectable<?>> injectables = extensionInitializer.initializePackages( packageNames );
            webServer.addJAXRSPackages( packageNames, tpp.getMountPoint(), injectables );
            log.info( "Mounted third-party JAX-RS package [%s] at [%s]", tpp.getPackageName(), tpp.getMountPoint() );
            if ( logger != null )
                logger.logMessage( String.format( "Mounted third-party JAX-RS package [%s] at [%s]",
                        tpp.getPackageName(), tpp.getMountPoint() ) );
        }
    }

    private List<String> packagesFor( ThirdPartyJaxRsPackage tpp )
    {
        return listFrom( new String[] { tpp.getPackageName() } );
    }

    @Override
	public void stop()
    {
        if ( packages == null )  return;

        for ( ThirdPartyJaxRsPackage tpp : packages )
        {
            webServer.removeJAXRSPackages( packagesFor( tpp ), tpp.getMountPoint() );
        }

        extensionInitializer.stop();
    }
}
