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
package org.neo4j.internal.kernel.api;

public abstract class DefaultCloseListenable implements AutoCloseablePlus
{
    protected CloseListener closeListener;
    private int token;

    public final void setCloseListener( CloseListener closeListener )
    {
        this.closeListener = closeListener;
    }

    public final CloseListener getCloseListener()
    {
        return this.closeListener;
    }

    @Override
    public final void close()
    {
        closeInternal();
        var listener = closeListener;
        if ( listener != null )
        {
            listener.onClosed( this );
        }
    }

    public final void setToken( int token )
    {
        this.token = token;
    }

    public final int getToken()
    {
        return token;
    }

    public static DefaultCloseListenable wrap( AutoCloseable c )
    {
        return new DefaultCloseListenable()
        {
            private boolean closed;

            @Override
            public void closeInternal()
            {
                try
                {
                    c.close();
                    closed = true;
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public boolean isClosed()
            {
                return closed;
            }
        };
    }
}
