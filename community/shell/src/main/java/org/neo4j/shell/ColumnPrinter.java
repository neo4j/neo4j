/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.shell;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ColumnPrinter
{
    private final List<Column> columns = new ArrayList<Column>();

    public ColumnPrinter( String... columnPrefixes )
    {
        for ( String prefix : columnPrefixes )
        {
            this.columns.add( new Column( prefix ) );
        }
    }

    public void add( Object... columns )
    {
        Iterator<Column> columnIterator = this.columns.iterator();
        for ( Object column : columns )
        {
            columnIterator.next().add( column.toString() );
        }
        if ( columnIterator.hasNext() )
        {
            throw new IllegalArgumentException( "Invalid column count " + columns.length + ", expected " +
                    this.columns.size() );
        }
    }

    public void print( Output out ) throws RemoteException
    {
        Column firstColumn = columns.get( 0 );
        for ( int line = 0; line < firstColumn.size(); line++ )
        {
            firstColumn.print( out, line );
            for ( int col = 1; col < columns.size(); col++ )
            {
                columns.get( col ).print( out, line );
            }
            out.println();
        }
    }

    private static class Column
    {
        private int widest = 0;
        private final List<String> cells = new ArrayList<String>();
        private final String prefix;

        public Column( String prefix )
        {
            this.prefix = prefix;
        }

        void add( String cell )
        {
            cells.add( cell );
            widest = Math.max( widest, cell.length() );
        }

        int size()
        {
            return cells.size();
        }

        void print( Output out, int i ) throws RemoteException
        {
            String value = cells.get( i );
            out.print( prefix + value + multiply( " ", widest - value.length() + 1 ) );
        }
    }

    private static String multiply( String string, int count ) throws RemoteException
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < count; i++ )
        {
            builder.append( string );
        }
        return builder.toString();
    }
}
