/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.server.ServerTestUtils.createTempDir;
import static org.neo4j.server.ServerTestUtils.writePropertyToFile;

import java.io.File;
import java.io.IOException;

public class DatabaseTuningPropertyFileBuilder
{
    private File parentDirectory = null;
    private String mappedMemory = null;

    public static DatabaseTuningPropertyFileBuilder builder()
    {
        return new DatabaseTuningPropertyFileBuilder();
    }

    private DatabaseTuningPropertyFileBuilder()
    {
    }

    public File build() throws IOException
    {
        if ( parentDirectory == null )
        {
            parentDirectory = createTempDir();
        }

        File temporaryConfigFile = new File( parentDirectory, "neo4j.properties" );

        if ( mappedMemory == null )
        {
            writePropertyToFile( "neostore.nodestore.db.mapped_memory", "25M", temporaryConfigFile );
        }
        else
        {
            writePropertyToFile( "neostore.nodestore.db.mapped_memory", mappedMemory, temporaryConfigFile );
        }
        writePropertyToFile( "neostore.relationshipstore.db.mapped_memory", "50M", temporaryConfigFile );
        writePropertyToFile( "neostore.propertystore.db.mapped_memory", "90M", temporaryConfigFile );
        writePropertyToFile( "neostore.propertystore.db.strings.mapped_memory", "130M", temporaryConfigFile );
        writePropertyToFile( "neostore.propertystore.db.arrays.mapped_memory", "150M", temporaryConfigFile );

        return temporaryConfigFile;
    }

    public DatabaseTuningPropertyFileBuilder inDirectory( File parentDirectory )
    {
        this.parentDirectory = parentDirectory;
        return this;
    }

    public DatabaseTuningPropertyFileBuilder mappedMemory( int i )
    {
        this.mappedMemory = String.valueOf( i ) + "M";
        return this;
    }

}
