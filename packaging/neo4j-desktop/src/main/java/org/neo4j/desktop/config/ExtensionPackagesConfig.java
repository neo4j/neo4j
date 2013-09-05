/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExtensionPackagesConfig implements Value<List<String>>
{
    private final File file;
    private final ListIO io = new FileSystemListIO();
    private final Environment environment;

    public ExtensionPackagesConfig( Environment environment )
    {
        this.environment = environment;
        this.file = new File( environment.getExtensionsDirectory(), "packages" );
    }

    @Override
    public List<String> get()
    {
        try
        {
            return io.read( new ArrayList<String>(), file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void set( List<String> value ) throws IllegalStateException
    {
        try
        {
            environment.getExtensionsDirectory().mkdirs();
            io.write( value, file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
