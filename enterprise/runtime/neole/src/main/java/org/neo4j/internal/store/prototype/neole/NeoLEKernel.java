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
package org.neo4j.internal.store.prototype.neole;

import java.io.IOException;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Permissions;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.Transaction;

public class NeoLEKernel implements Kernel
{
    private final CursorFactory cursorFactory;
    private final ReadStore readStore;

    public NeoLEKernel( ReadStore readStore )
    {
        this.readStore = readStore;
        this.cursorFactory = new CursorFactory( readStore );
    }

    @Override
    public CursorFactory cursors()
    {
        return cursorFactory;
    }

    @Override
    public Session beginSession( Permissions permissions )
    {
        return new Session()
        {

            @Override
            public Transaction beginTransaction()
            {
                try
                {
                    return new NeoLETransaction( readStore );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "failed" );
                }
            }

            @Override
            public Token token()
            {
                return null;
            }

            @Override
            public void close()
            {
            }
        };
    }
}
