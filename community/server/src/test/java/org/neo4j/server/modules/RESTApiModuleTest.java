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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.TransactionRegistry;
import org.neo4j.server.web.WebServer;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class RESTApiModuleTest
{
    @Test
    public void shouldRegisterASingleUri() throws Exception
    {
        WebServer webServer = mock( WebServer.class );

        Configuration config = new PropertiesConfiguration();
        String path = "/db/data";
        config.addProperty( Configurator.REST_API_PATH_PROPERTY_KEY, path );

        Database db = mock(Database.class);
        when(db.getTransactionRegistry()).thenReturn(mock(TransactionRegistry.class));

        RESTApiModule module = new RESTApiModule(webServer, db, config);
        module.start(StringLogger.DEV_NULL);

        verify( webServer ).addJAXRSPackages( any( List.class ), anyString(), anyCollection() );
    }
}
