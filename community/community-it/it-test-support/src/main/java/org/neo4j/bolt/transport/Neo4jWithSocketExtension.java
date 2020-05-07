/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.FileSystemExtension;
import org.neo4j.test.extension.StatefulFieldExtension;

import static org.neo4j.test.extension.FileSystemExtension.FILE_SYSTEM;
import static org.neo4j.test.extension.FileSystemExtension.FILE_SYSTEM_NAMESPACE;

public class Neo4jWithSocketExtension extends StatefulFieldExtension<Neo4jWithSocket>
{
    private static final String FIELD_KEY = "neo4jWithSocket";
    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create( "org", "neo4j", FIELD_KEY );

    @Override
    protected String getFieldKey()
    {
        return FIELD_KEY;
    }

    @Override
    protected Class<Neo4jWithSocket> getFieldType()
    {
        return Neo4jWithSocket.class;
    }

    @Override
    protected Neo4jWithSocket createField( ExtensionContext context )
    {
        var fs = getFileSystem( context );
        return new Neo4jWithSocket( context.getRequiredTestClass(), new TestDatabaseManagementServiceBuilder(), () -> fs, settings -> {} );
    }

    public FileSystemAbstraction getFileSystem( ExtensionContext context )
    {
        var fs = context.getStore( FILE_SYSTEM_NAMESPACE ).get( FILE_SYSTEM, FileSystemAbstraction.class );
        if ( fs == null )
        {
            var fsClassName = FileSystemExtension.class.getSimpleName();
            var neo4JClassName = getClass().getSimpleName();
            throw new IllegalStateException( fsClassName + " not in scope, make sure to add it before " + neo4JClassName );
        }
        return fs;
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return NAMESPACE;
    }
}
