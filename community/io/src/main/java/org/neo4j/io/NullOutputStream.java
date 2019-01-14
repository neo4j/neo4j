/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io;

import java.io.IOException;
import java.io.OutputStream;

public class NullOutputStream extends OutputStream
{
    public static final OutputStream NULL_OUTPUT_STREAM = new NullOutputStream();

    private NullOutputStream()
    {
    }

    @Override
    public void write( int b )
    {
        //nothing
    }

    @Override
    public void write( byte[] b )
    {
        // nothing
    }

    @Override
    public void write( byte[] b, int off, int len )
    {
        // nothing
    }
}
