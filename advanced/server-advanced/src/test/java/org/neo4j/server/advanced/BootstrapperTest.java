/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.advanced;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.server.advanced.helpers.AdvancedServerBuilder;
import org.neo4j.server.advanced.jmx.ServerManagement;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertNotNull;

public class BootstrapperTest
{
    @Test
    public void shouldBeAbleToRestartServer() throws Exception
    {
        TargetDirectory target = TargetDirectory.forTest( getClass() );
        String dbDir1 = target.cleanDirectory( "db1" ).getAbsolutePath();
        Configurator config = new PropertyFileConfigurator( Validator.NO_VALIDATION,
                AdvancedServerBuilder
                        .server()
                        .usingDatabaseDir( dbDir1 )
                        .createPropertiesFiles(), ConsoleLogger.DEV_NULL );

        // TODO: This needs to be here because of a startuphealthcheck
        // that requires this system property. Look into moving
        // config file check into bootstrapper to avoid this.
        File irrelevant = target.file( "irrelevant" );
        irrelevant.createNewFile();

        config.configuration().setProperty( "org.neo4j.server.properties", irrelevant.getAbsolutePath());

        AdvancedNeoServer server = new AdvancedNeoServer( config,
                new SingleLoggingService( StringLogger.SYSTEM ) );

        server.start( );

        assertNotNull( server.getDatabase().getGraph() );

        // Change the database location
        String dbDir2 = target.cleanDirectory( "db2" ).getAbsolutePath();

        Configuration conf = config.configuration();
        conf.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir2 );

        ServerManagement bean = new ServerManagement( server );
        bean.restartServer();

    }
}
