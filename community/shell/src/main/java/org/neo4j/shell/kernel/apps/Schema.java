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

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.indexOf;
import static org.neo4j.helpers.collection.Iterables.sort;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.shell.Continuation.INPUT_COMPLETE;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
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
    private static final Label[] EMPTY_LABELS = new Label[0];
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
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.MUST,
                "Specifies which label selected operation is about" ) );
        addOptionDefinition( "p", new OptionDefinition( OptionValueType.MUST,
                "Specifies which property selected operation is about" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Accesses db schema. Usage: schema <action> <options...>\n" +
                "Listing indexes\n" +
                "  schema ls\n" +
                "  schema ls -l :Person\n" +
                "Awaiting indexes to come online\n" +
                "  schema await -l Person -p name";
    }
    
    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        String action = parser.argumentWithDefault( 0, "ls" );
        org.neo4j.graphdb.schema.Schema schema = getServer().getDb().schema();
        Label[] labels = parseLabels( parser );
        String property = parser.option( "p", null );
        
        if ( action.equals( "await" ) )
            // Await an index to come online
            awaitIndexes( out, schema, labels, property );
        else if ( action.equals( "ls" ) )
            // List schema rules (currently only indexes)
            listIndexes( out, schema, labels, property );
        
        return INPUT_COMPLETE;
    }

    private void awaitIndexes( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels, String property )
            throws RemoteException
    {
        for ( IndexDefinition index : indexesByLabelAndProperty( schema, labels, property ) )
        {
            if ( schema.getIndexState( index ) != IndexState.ONLINE )
            {
                out.println( "Awaiting :" + index.getLabel().name() + " ON " +
                        toList( index.getPropertyKeys() ) + " " + IndexState.ONLINE );
                schema.awaitIndexOnline( index, 10000, TimeUnit.DAYS );
            }
        }
    }

    private void listIndexes( Output out, org.neo4j.graphdb.schema.Schema schema, Label[] labels,
            final String property ) throws RemoteException
    {
        ColumnPrinter printer = new ColumnPrinter( "  :", "ON ", "" );
        Iterable<IndexDefinition> indexes = indexesByLabelAndProperty( schema, labels, property );

        int i = 0;
        for ( IndexDefinition index : sort( indexes, LABEL_COMPARE_FUNCTION ) )
        {
            if ( i == 0 )
                out.println( "Indexes" );
            printer.add( index.getLabel().name(), toList( index.getPropertyKeys() ),
                    schema.getIndexState( index ) );
            i++;
        }
        if ( i == 0 )
            out.println( "No indexes" );
        
        int j = 0;
        for ( ConstraintDefinition constraint : constraintsByLabelAndProperty( schema, labels, property ) )
        {
            if ( j == 0 ) out.println( "Constraints" );
            printer.add( constraint.getLabel().name(), constraint.getConstraintType().name(), constraint.toString() );
            j++;

        }
        if ( j == 0 ) out.println( "No constraints" );

        printer.print( out );
    }

    private Iterable<IndexDefinition> indexesByLabelAndProperty( org.neo4j.graphdb.schema.Schema schema,
            Label[] labels, final String property )
    {
        Iterable<IndexDefinition> indexes = indexesByLabel( schema, labels );
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

    private Iterable<ConstraintDefinition> constraintsByLabelAndProperty( org.neo4j.graphdb.schema.Schema schema,
            final Label[] labels, final String property )
    {
        Iterable<ConstraintDefinition> constraints = filter( new Predicate<ConstraintDefinition>()
        {
            @Override
            public boolean accept( ConstraintDefinition constraint )
            {
                if ( hasLabel( constraint, labels ) )
                {
                    return isMatchingConstraint( constraint, property );
                }
                else
                {
                    return false;
                }
            }
        }, schema.getConstraints() );

        return constraints;
    }

    private boolean hasLabel( ConstraintDefinition constraint, Label[] labels )
    {
        if ( labels.length == 0 )
            return true;

        for ( Label label : labels )
        {
            if ( constraint.getLabel().name().equals( label.name() ) )
            {
                return true;
            }
        }
        
        return false;
    }

    private boolean isMatchingConstraint( ConstraintDefinition constraint, final String property )
    {
        if ( property == null )
        {
            return true;
        }

        switch ( constraint.getConstraintType() )
        {
        case UNIQUENESS:
            UniquenessConstraintDefinition typedConstraint = constraint.asUniquenessConstraint();
            return indexOf( property, typedConstraint.getPropertyKeys() ) != -1;
        default:
            return false;
        }
    }
    
    private Iterable<IndexDefinition> indexesByLabel( org.neo4j.graphdb.schema.Schema schema, Label[] labels )
    {
        Iterable<IndexDefinition> indexes = schema.getIndexes();
        for ( final Label label : labels )
        {
            indexes = filter( new Predicate<IndexDefinition>()
            {
                @Override
                public boolean accept( IndexDefinition item )
                {
                    return item.getLabel().name().equals( label.name() );
                }
            }, indexes );
        }
        return indexes;
    }
}
