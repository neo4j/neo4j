/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

public class StoreFiles
{
    public static final String[] fileNames = {
            "neostore",
            "neostore.nodestore.db",
            "neostore.propertystore.db",
            "neostore.propertystore.db.arrays",
            "neostore.propertystore.db.index",
            "neostore.propertystore.db.index.keys",
            "neostore.propertystore.db.strings",
            "neostore.relationshipstore.db",
            "neostore.relationshiptypestore.db",
            "neostore.relationshiptypestore.db.names",
    };

    public static void move( File fromDirectory, File toDirectory ) throws IOException
    {
        // TODO: change the order that files are moved to handle failure conditions properly
        for ( String fileName : fileNames )
        {
            moveFile( fileName, fromDirectory, toDirectory );
            moveFile( fileName + ".id", fromDirectory, toDirectory );
        }
    }

    private static void moveFile( String fileName, File fromDirectory, File toDirectory )
    {
        new File( fromDirectory, fileName ).renameTo( new File( toDirectory, fileName ) );
    }
}
