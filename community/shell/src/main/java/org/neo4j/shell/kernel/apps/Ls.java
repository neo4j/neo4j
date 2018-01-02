/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.impl.util.SingleNodePath;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.ColumnPrinter;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. lists
 * properties/relationships on a node or a relationship.
 */
@Service.Implementation( App.class )
public class Ls extends TransactionProvidingApp
{
    private static final int DEFAULT_MAX_RELS_PER_TYPE_LIMIT = 10;
    
    {
        addOptionDefinition( "b", new OptionDefinition( OptionValueType.NONE,
            "Brief summary instead of full content" ) );
        addOptionDefinition( "v", new OptionDefinition( OptionValueType.NONE,
            "Verbose mode" ) );
        addOptionDefinition( "q", new OptionDefinition( OptionValueType.NONE, "Quiet mode" ) );
        addOptionDefinition( "p", new OptionDefinition( OptionValueType.NONE,
            "Lists properties" ) );
        addOptionDefinition( "r", new OptionDefinition( OptionValueType.NONE,
            "Lists relationships" ) );
        addOptionDefinition( "f", new OptionDefinition( OptionValueType.MUST,
            "Filters property keys/values and relationship types. Supplied either as a single value " +
            "or as a JSON string where both keys and values can contain regex. " +
            "Starting/ending {} brackets are optional. Examples:\n" +
            "  \"username\"\n\tproperty/relationship 'username' gets listed\n" +
            "  \".*name:ma.*, age:''\"\n\tproperties with keys matching '.*name' and values matching 'ma.*' " +
            "gets listed,\n\tas well as the 'age' property. Also relationships matching '.*name' or 'age'\n\tgets listed\n" +
            "  \"KNOWS:out,LOVES:in\"\n\toutgoing KNOWS and incoming LOVES relationships gets listed" ) );
        addOptionDefinition( "i", new OptionDefinition( OptionValueType.NONE,
            "Filters are case-insensitive (case-sensitive by default)" ) );
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.NONE,
            "Filters matches more loosely, i.e. it's considered a match if just " +
            "a part of a value matches the pattern, not necessarily the whole value" ) );
        addOptionDefinition( "s", new OptionDefinition( OptionValueType.NONE,
            "Sorts relationships by type." ) );
        addOptionDefinition( "m", new OptionDefinition( OptionValueType.MAY,
            "Display a maximum of M relationships per type (default " + DEFAULT_MAX_RELS_PER_TYPE_LIMIT + " if no value given)" ) );
        addOptionDefinition( "a", new OptionDefinition( OptionValueType.NONE,
            "Allows for cd:ing to a node not connected to the current node (e.g. 'absolute')" ) );
    }

    @Override
    public String getDescription()
    {
        return "Lists the contents of the current node or relationship. " +
        	"Optionally supply\n" +
            "node id for listing a certain node using \"ls <node-id>\"";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        boolean brief = parser.options().containsKey( "b" );
        boolean verbose = parser.options().containsKey( "v" );
        boolean quiet = parser.options().containsKey( "q" );
        if ( verbose && quiet )
        {
            verbose = false;
            quiet = false;
        }
        boolean displayProperties = parser.options().containsKey( "p" );
        boolean displayRelationships = parser.options().containsKey( "r" );
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        Map<String, Object> filterMap = parseFilter( parser.options().get( "f" ), out );
        if ( !displayProperties && !displayRelationships )
        {
            displayProperties = true;
            displayRelationships = true;
        }

        NodeOrRelationship thing = null;
        if ( parser.arguments().isEmpty() )
        {
            thing = this.getCurrent( session );
        }
        else
        {
            thing = NodeOrRelationship.wrap( this.getNodeById( Long
                .parseLong( parser.arguments().get( 0 ) ) ) );
        }

        if ( displayProperties )
        {
            displayLabels( thing, out, filterMap, caseInsensitiveFilters, looseFilters, brief );
            displayProperties( thing, out, verbose, quiet, filterMap, caseInsensitiveFilters,
                    looseFilters, brief );
        }
        if ( displayRelationships )
        {
            if ( thing.isNode() )
            {
                displayRelationships( parser, thing, session, out, verbose, quiet,
                        filterMap, caseInsensitiveFilters, looseFilters, brief );
            }
            else
            {
                displayNodes( parser, thing, session, out );
            }
        }
        return Continuation.INPUT_COMPLETE;
    }

    private void displayNodes( AppCommandParser parser, NodeOrRelationship thing,
            Session session, Output out ) throws RemoteException, ShellException
    {
        Relationship rel = thing.asRelationship();
        out.println( getDisplayName( getServer(), session, rel.getStartNode(), false ) +
                " --" + getDisplayName( getServer(), session, rel, true, false ) + "-> " +
                getDisplayName( getServer(), session, rel.getEndNode(), false ) );
    }

    private Iterable<String> sortKeys( Iterable<String> source )
    {
        List<String> list = new ArrayList<String>();
        for ( String item : source )
        {
            list.add( item );
        }
        Collections.sort( list, new Comparator<String>()
        {
            @Override
            public int compare( String item1, String item2 )
            {
                return item1.toLowerCase().compareTo( item2.toLowerCase() );
            }
        } );
        return list;
    }

    private void displayProperties( NodeOrRelationship thing, Output out,
        boolean verbose, boolean quiet, Map<String, Object> filterMap,
        boolean caseInsensitiveFilters, boolean looseFilters, boolean brief )
        throws RemoteException
    {
        ColumnPrinter columnPrinter = quiet ?
                new ColumnPrinter( "*" ) :
                new ColumnPrinter( "*", "=" );
        int count = 0;
        for ( String key : sortKeys( thing.getPropertyKeys() ) )
        {
            Object value = thing.getProperty( key );
            if ( !filterMatches( filterMap, caseInsensitiveFilters, looseFilters, key, value ) )
            {
                continue;
            }

            count++;
            if ( !brief )
            {
                if ( quiet )
                {
                    columnPrinter.add( key );
                }
                else
                {
                    columnPrinter.add( key, verbose ?
                            format( value, true ) + " (" + getNiceType( value ) + ")" :
                            format( value, true ) );
                }
            }
        }
        columnPrinter.print( out );
        if ( brief )
        {
            out.println( "Property count: " + count );
        }
    }

    private void displayLabels( NodeOrRelationship thing, Output out, Map<String, Object> filterMap,
            boolean caseInsensitiveFilters, boolean looseFilters, boolean brief ) throws RemoteException
    {
        List<String> labelNames = new ArrayList<String>();
        for ( Label label : thing.asNode().getLabels() )
            labelNames.add( label.name() );
        
        if ( brief )
        {
            out.println( "Label count: " + labelNames.size() );
        }
        else
        {
            for ( String label : sortKeys( labelNames ) )
            {
                if ( filterMatches( filterMap, caseInsensitiveFilters, looseFilters, label, "" ) )
                    out.println( ":" + label );
            }
        }
    }
    
    private void displayRelationships( AppCommandParser parser, NodeOrRelationship thing,
        Session session, Output out, boolean verbose, boolean quiet,
        Map<String, Object> filterMap, boolean caseInsensitiveFilters,
        boolean looseFilters, boolean brief ) throws ShellException, RemoteException
    {
        boolean sortByType = parser.options().containsKey( "s" );
        Node node = thing.asNode();
        Iterable<Relationship> relationships = getRelationships( node, filterMap,
                caseInsensitiveFilters, looseFilters, sortByType|brief );
        if ( brief )
        {
            Iterator<Relationship> iterator = relationships.iterator();
            if ( !iterator.hasNext() )
            {
                return;
            }

            Relationship sampleRelationship = iterator.next();
            RelationshipType lastType = sampleRelationship.getType();
            int currentCounter = 1;
            while ( iterator.hasNext() )
            {
                Relationship rel = iterator.next();
                if ( !rel.isType( lastType ) )
                {
                    displayBriefRelationships( thing, session, out, sampleRelationship, currentCounter );
                    sampleRelationship = rel;
                    lastType = sampleRelationship.getType();
                    currentCounter = 1;
                }
                else
                {
                    currentCounter++;
                }
            }
            displayBriefRelationships( thing, session, out, sampleRelationship, currentCounter );
        }
        else
        {
            Iterator<Relationship> iterator = relationships.iterator();
            if ( parser.options().containsKey( "m" ) )
            {
                iterator = wrapInLimitingIterator( parser, iterator, filterMap, caseInsensitiveFilters, looseFilters );
            }
            
            while ( iterator.hasNext() )
            {
                Relationship rel = iterator.next();
                StringBuffer buf = new StringBuffer( getDisplayName(
                        getServer(), session, thing, true ) );
                String relDisplay = quiet ? "" : getDisplayName( getServer(), session, rel, verbose, true );
                buf.append( withArrows( rel, relDisplay, thing.asNode() ) );
                buf.append( getDisplayName( getServer(), session, rel.getOtherNode( node ), true ) );
                out.println( buf );
            }
        }
    }
    
    private Iterator<Relationship> wrapInLimitingIterator( AppCommandParser parser,
            Iterator<Relationship> iterator, Map<String, Object> filterMap, boolean caseInsensitiveFilters,
            boolean looseFilters ) throws ShellException
    {
        final AtomicBoolean handBreak = new AtomicBoolean();
        int maxRelsPerType = parser.optionAsNumber( "m", DEFAULT_MAX_RELS_PER_TYPE_LIMIT ).intValue();
        Map<String, Direction> types = filterMapToTypes( getServer().getDb(),
                Direction.BOTH, filterMap, caseInsensitiveFilters, looseFilters );
        return new FilteringIterator<Relationship>( iterator,
                new LimitPerTypeFilter( maxRelsPerType, types, handBreak ) )
        {
            @Override
            protected Relationship fetchNextOrNull()
            {
                return handBreak.get() ? null : super.fetchNextOrNull();
            }
        };
    }

    private static class LimitPerTypeFilter implements Predicate<Relationship>
    {
        private final int maxRelsPerType;
        private final Map<String, AtomicInteger> encounteredRelationships = new HashMap<String, AtomicInteger>();
        private int typesMaxedOut = 0;
        private final AtomicBoolean iterationHalted;

        public LimitPerTypeFilter( int maxRelsPerType, Map<String, Direction> types, AtomicBoolean handBreak )
        {
            this.maxRelsPerType = maxRelsPerType;
            this.iterationHalted = handBreak;
            for ( String type : types.keySet() )
            {
                encounteredRelationships.put( type, new AtomicInteger() );
            }
        }

        @Override
        public boolean test( Relationship item )
        {
            AtomicInteger counter = encounteredRelationships.get( item.getType().name() );
            int count = counter.get();
            if ( count < maxRelsPerType )
            {
                if ( counter.incrementAndGet() == maxRelsPerType )
                {
                    counter.incrementAndGet();
                    if ( (++typesMaxedOut) >= encounteredRelationships.size() )
                    {
                        iterationHalted.set( true );
                    }
                    return true;
                }
                return true;
            }
            return false;
        }
    }

    private Iterable<Relationship> getRelationships( final Node node, Map<String, Object> filterMap,
            boolean caseInsensitiveFilters, boolean looseFilters, boolean sortByType ) throws ShellException
    {
        if ( sortByType )
        {
            Path nodeAsPath = new SingleNodePath( node );
            return toSortedExpander( getServer().getDb(), Direction.BOTH, filterMap,
                    caseInsensitiveFilters, looseFilters ).expand( nodeAsPath, BranchState.NO_STATE );
        }
        else
        {
            if ( filterMap.isEmpty() )
            {
                return node.getRelationships();
            }
            else
            {
                Path nodeAsPath = new SingleNodePath( node );
                return toExpander( getServer().getDb(), Direction.BOTH, filterMap, caseInsensitiveFilters, looseFilters ).expand( nodeAsPath, BranchState.NO_STATE );
            }
        }
    }

    private void displayBriefRelationships( NodeOrRelationship thing, Session session, Output out,
            Relationship sampleRelationship, int count ) throws ShellException,
            RemoteException
    {
        String relDisplay = withArrows( sampleRelationship, getDisplayName( getServer(), session,
                sampleRelationship, false, true ), thing.asNode() );
        out.println( getDisplayName( getServer(), session, thing, true ) + relDisplay + " x" + count );
    }

    private static String getNiceType( Object value )
    {
        return Set.getValueTypeName( value.getClass() );
    }
}
