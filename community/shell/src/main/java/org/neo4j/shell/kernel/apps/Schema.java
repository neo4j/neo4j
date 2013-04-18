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
package org.neo4j.shell.kernel.apps;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.indexOf;
import static org.neo4j.helpers.collection.Iterables.sort;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.shell.Continuation.INPUT_COMPLETE;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.ColumnPrinter;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;

public class Schema extends GraphDatabaseApp
{
    private static final Function<IndexDefinition, String> LABEL_COMPARE_FUNCTION =
            new Function<IndexDefinition, String>()
    {
        @Override
        public String apply( IndexDefinition index )
        {
            return index.getLabel().name();
        }
    };
    
    {
        addOptionDefinition( "ls", new OptionDefinition( OptionValueType.NONE, "Lists all schema rules" ) );
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.MUST,
                "Specifies which label selected operation is about" ) );
        addOptionDefinition( "p", new OptionDefinition( OptionValueType.MUST,
                "Specifies which property selected operation is about" ) );
        addOptionDefinition( "await", new OptionDefinition( OptionValueType.NONE,
                "Awaits indexes matching given label and property" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Accesses db schema";
    }
    
    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        org.neo4j.graphdb.schema.Schema schema = getServer().getDb().schema();
        String label = parser.option( "l", null );
        String property = parser.option( "p", null );
        
        // List schema rules (currently only indexes)
        if ( parser.options().containsKey( "ls" ) )
        {
            listIndexes( out, schema, label, property );
        }
        
        // Await an index to become online
        if ( parser.options().containsKey( "await" ) )
        {
            awaitIndexes( out, schema, label, property );
        }
        
        return INPUT_COMPLETE;
    }

    private void awaitIndexes( Output out, org.neo4j.graphdb.schema.Schema schema, String label, String property )
            throws RemoteException
    {
        for ( IndexDefinition index : indexesByLabelAndProperty( schema, label, property ) )
        {
            if ( schema.getIndexState( index ) != IndexState.ONLINE )
            {
                out.println( "Awaiting :" + index.getLabel().name() + " ON " +
                        toList( index.getPropertyKeys() ) + " " + IndexState.ONLINE );
                schema.awaitIndexOnline( index, 10000, TimeUnit.DAYS );
            }
        }
    }

    private void listIndexes( Output out, org.neo4j.graphdb.schema.Schema schema, String label,
            final String property ) throws RemoteException
    {
        ColumnPrinter printer = new ColumnPrinter( "  :", "ON ", "" );
        Iterable<IndexDefinition> indexes = indexesByLabelAndProperty( schema, label, property );
                
        out.println( "Indexes" );
        for ( IndexDefinition index : sort( indexes, LABEL_COMPARE_FUNCTION ) )
        {
            printer.add( index.getLabel().name(), toList( index.getPropertyKeys() ),
                    schema.getIndexState( index ) );
        }
        printer.print( out );
    }

    private Iterable<IndexDefinition> indexesByLabelAndProperty( org.neo4j.graphdb.schema.Schema schema, String label,
            final String property )
    {
        Iterable<IndexDefinition> indexes = label != null ?
                schema.getIndexes( label( label ) ) :
                schema.getIndexes();
        if ( property != null )
        {
            indexes = filter( new Predicate<IndexDefinition>()
            {
                @Override
                public boolean accept( IndexDefinition index )
                {
                    return indexOf( property, index.getPropertyKeys() ) != -1;
                }
            }, indexes );
        }
        return indexes;
    }
}
