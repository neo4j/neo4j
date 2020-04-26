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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import org.neo4j.io.fs.FileSystemAbstraction;

public abstract class FileSystemExtension<T extends FileSystemAbstraction> extends StatefulFieldExtension<T>
{
    public static final String FILE_SYSTEM = "fileSystem";
    public static final Namespace FILE_SYSTEM_NAMESPACE = Namespace.create( FILE_SYSTEM );

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        T storedValue = getStoredValue( context );
        if ( storedValue != null )
        {
            storedValue.close();
        }
        super.afterAll( context );
    }

    @Override
    protected String getFieldKey()
    {
        return FILE_SYSTEM;
    }

    @Override
    protected Namespace getNameSpace()
    {
        return FILE_SYSTEM_NAMESPACE;
    }
}
