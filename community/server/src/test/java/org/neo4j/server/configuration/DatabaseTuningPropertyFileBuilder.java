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
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;

import static org.neo4j.server.ServerTestUtils.writePropertiesToFile;

public class DatabaseTuningPropertyFileBuilder
{
    private File parentDirectory = null;
    private String mappedMemory = null;
    private String kernelId;

    public static DatabaseTuningPropertyFileBuilder builder( File directory )
    {
        return new DatabaseTuningPropertyFileBuilder( directory );
    }

    private DatabaseTuningPropertyFileBuilder( File directory )
    {
        this.parentDirectory = directory;
    }

    public File build() throws IOException
    {
        File temporaryConfigFile = new File( parentDirectory, "neo4j.properties" );
        Map<String, String> properties = MapUtil.stringMap(
                "neostore.relationshipstore.db.mapped_memory", "50M",
                "neostore.propertystore.db.mapped_memory", "90M",
                "neostore.propertystore.db.strings.mapped_memory", "130M",
                "neostore.propertystore.db.arrays.mapped_memory", "150M" );
        if ( mappedMemory == null )
        {
            properties.put( "neostore.nodestore.db.mapped_memory", "25M" );
        }
        else
        {
            properties.put( "neostore.nodestore.db.mapped_memory", mappedMemory );
        }


        if(kernelId != null)
        {
            properties.put( GraphDatabaseSettings.forced_kernel_id.name(), kernelId );
        }

        writePropertiesToFile( properties, temporaryConfigFile );
        return temporaryConfigFile;
    }

    public DatabaseTuningPropertyFileBuilder mappedMemory( int i )
    {
        this.mappedMemory = String.valueOf( i ) + "M";
        return this;
    }

    public DatabaseTuningPropertyFileBuilder withKernelId(String kernelId)
    {
        this.kernelId = kernelId;
        return this;
    }
}
