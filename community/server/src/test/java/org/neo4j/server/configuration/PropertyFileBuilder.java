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
package org.neo4j.server.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.ServerTestUtils;

public class PropertyFileBuilder
{
    private final String portNo = "7474";
    private final String webAdminUri = "http://localhost:7474/db/manage/";
    private final String webAdminDataUri = "http://localhost:7474/db/data/";
    private String dbTuningPropertyFile = null;
    private final ArrayList<Tuple> nameValuePairs = new ArrayList<Tuple>();
    private final File directory;

    private static class Tuple
    {
        public Tuple( String name, String value )
        {
            this.name = name;
            this.value = value;
        }

        public String name;
        public String value;
    }

    public static PropertyFileBuilder builder( File directory )
    {
        return new PropertyFileBuilder( directory );
    }

    private PropertyFileBuilder( File directory )
    {
        this.directory = directory;
    }

    public File build() throws IOException
    {
        File file = new File( directory, "config" );
        Map<String, String> properties = MapUtil.stringMap(
                Configurator.DATABASE_LOCATION_PROPERTY_KEY, directory.getAbsolutePath(),
                Configurator.RRDB_LOCATION_PROPERTY_KEY, directory.getAbsolutePath(),
                Configurator.MANAGEMENT_PATH_PROPERTY_KEY, webAdminUri,
                Configurator.REST_API_PATH_PROPERTY_KEY, webAdminDataUri );
        if ( portNo != null )
            properties.put( Configurator.WEBSERVER_PORT_PROPERTY_KEY, portNo );
        if ( dbTuningPropertyFile != null )
            properties.put( Configurator.DB_TUNING_PROPERTY_FILE_KEY, dbTuningPropertyFile );
        for ( Tuple t : nameValuePairs )
            properties.put( t.name, t.value );
        ServerTestUtils.writePropertiesToFile( properties, file );
        return file;
    }

    public PropertyFileBuilder withDbTuningPropertyFile( File f )
    {
        dbTuningPropertyFile = f.getAbsolutePath();
        return this;
    }

    public PropertyFileBuilder withNameValue( String name, String value )
    {
        nameValuePairs.add( new Tuple( name, value ) );
        return this;
    }

    public PropertyFileBuilder withDbTuningPropertyFile( String propertyFileLocation )
    {
        dbTuningPropertyFile = propertyFileLocation;
        return this;
    }
}
