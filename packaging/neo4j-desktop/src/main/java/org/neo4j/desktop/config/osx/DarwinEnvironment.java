/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.desktop.config.osx;

import java.io.File;
import java.io.IOException;

import org.neo4j.desktop.config.portable.Environment;

class DarwinEnvironment extends Environment
{
    @Override
    public void editFile( File file ) throws IOException, SecurityException
    {
        try
        {
            super.editFile( file );
        }
        catch ( IOException | UnsupportedOperationException ex )
        {
            Runtime.getRuntime().exec( new String[]{"open", "-nt", file.getAbsolutePath()} );
        }
    }

    @Override
    public void openCommandPrompt( File binDirectory, File jreBinDirectory, File workingDirectory ) throws IOException
    {
        Runtime.getRuntime().exec( new String[] { "open", "-na", "Terminal", "openNeoTerminal.sh" } );
    }
}
