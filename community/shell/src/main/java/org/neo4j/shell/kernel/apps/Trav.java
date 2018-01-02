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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.CommonBranchOrdering;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

import static java.lang.Integer.parseInt;

import static org.neo4j.graphdb.traversal.Evaluators.toDepth;

/**
 * Traverses the graph using {@link Traverser}.
 */
@Service.Implementation( App.class )
public class Trav extends TransactionProvidingApp
{
    /**
     * Constructs a new command which can traverse the graph.
     */
    public Trav()
    {
        this.addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
            "The traversal order [BREADTH_FIRST/DEPTH_FIRST/breadth/depth]" ) );
        this.addOptionDefinition( "r", new OptionDefinition( OptionValueType.MUST,
            "The relationship type(s) expressed as a JSON string " +
            "(supports regex matching of the types) f.ex. " +
            "\"MY_REL_TYPE:out,.*_HAS_.*:both\". Matching is case-insensitive." ) );
        this.addOptionDefinition( "f", new OptionDefinition( OptionValueType.MUST,
            "Filters node property keys/values. Supplied either as a single " +
            "value or as a JSON string where both keys and values can " +
            "contain regex. Starting/ending {} brackets are optional. Examples:\n" +
            "\"username\"\n" +
            "   nodes which has property 'username' gets listed\n" +
            "\".*name: ma.*, age: ''\"\n" +
            "   nodes which has any key matching '.*name' where the " +
            "property value\n" +
            "   for that key matches 'ma.*' AND has the 'age' property gets listed" ) );
        this.addOptionDefinition( "i", new OptionDefinition( OptionValueType.NONE,
            "Filters are case-insensitive (case-sensitive by default)" ) );
        this.addOptionDefinition( "l", new OptionDefinition( OptionValueType.NONE,
            "Filters matches more loosely, i.e. it's considered a match if " +
            "just a part of a value matches the pattern, not necessarily " +
            "the whole value" ) );
        this.addOptionDefinition( "c", OPTION_DEF_FOR_C );
        this.addOptionDefinition( "d", new OptionDefinition( OptionValueType.MUST,
                "Depth limit" ) );
        this.addOptionDefinition( "u", new OptionDefinition( OptionValueType.MUST,
                "Uniqueness of the entities encountered during traversal " +
                niceEnumAlternatives( Uniqueness.class ) ) );
    }

    @Override
    public String getDescription()
    {
    	return "Traverses the graph from your current position (pwd). " +
    		"It's a reflection of the neo4j traverser API with some options for filtering " +
    		"which nodes will be returned.";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        assertCurrentIsNode( session );

        Node node = this.getCurrent( session ).asNode();
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        boolean quiet = parser.options().containsKey( "q" );
        
        // Order
        TraversalDescription description = Traversal.description();
        String order = parser.options().get( "o" );
        if ( order != null )
        {
            description = description.order( parseOrder( order ) );
        }
        
        // Relationship types / expander
        String relationshipTypes = parser.options().get( "r" );
        if ( relationshipTypes != null )
        {
            Map<String, Object> types = parseFilter( relationshipTypes, out );
            description = description.expand( toExpander( getServer().getDb(), null, types,
                    caseInsensitiveFilters, looseFilters ) );
        }
        
        // Uniqueness
        String uniqueness = parser.options().get( "u" );
        if ( uniqueness != null )
        {
            description = description.uniqueness( parseUniqueness( uniqueness ) );
        }
        
        // Depth limit
        String depthLimit = parser.options().get( "d" );
        if ( depthLimit != null )
        {
            description = description.evaluator( toDepth( parseInt( depthLimit ) ) );
        }

        String filterString = parser.options().get( "f" );
        Map<String, Object> filterMap = filterString != null ? parseFilter( filterString, out ) : null;
        String commandToRun = parser.options().get( "c" );
        Collection<String> commandsToRun = new ArrayList<String>();
        if ( commandToRun != null )
        {
            commandsToRun.addAll( Arrays.asList( commandToRun.split( Pattern.quote( "&&" ) ) ) );
        }
        for ( Path path : description.traverse( node ) )
        {
            boolean hit = false;
            if ( filterMap == null )
            {
                hit = true;
            }
            else
            {
                Node endNode = path.endNode();
                Map<String, Boolean> matchPerFilterKey = new HashMap<String, Boolean>();
                for ( String key : endNode.getPropertyKeys() )
                {
                    for ( Map.Entry<String, Object> filterEntry :
                        filterMap.entrySet() )
                    {
                        String filterKey = filterEntry.getKey();
                        if ( matchPerFilterKey.containsKey( filterKey ) )
                        {
                            continue;
                        }

                        if ( matches( newPattern( filterKey,
                            caseInsensitiveFilters ), key,
                            caseInsensitiveFilters, looseFilters ) )
                        {
                            Object value = endNode.getProperty( key );
                            String filterPattern =
                                filterEntry.getValue() != null ?
                                filterEntry.getValue().toString() : null;
                            if ( matches( newPattern( filterPattern,
                                caseInsensitiveFilters ), value.toString(),
                                caseInsensitiveFilters, looseFilters ) )
                            {
                                matchPerFilterKey.put( filterKey, true );
                            }
                        }
                    }
                }

                if ( matchPerFilterKey.size() == filterMap.size() )
                {
                    hit = true;
                }
            }
            if ( hit )
            {
                if ( commandsToRun.isEmpty() )
                {
                    printPath( path, quiet, session, out );
                }
                else
                {
                    printAndInterpretTemplateLines( commandsToRun, false, true,
                            NodeOrRelationship.wrap( path.endNode() ), getServer(), session, out );
                }
            }
        }
        return Continuation.INPUT_COMPLETE;
    }

    private UniquenessFactory parseUniqueness( String uniqueness )
    {
        return parseEnum( Uniqueness.class, uniqueness, null );
    }

    private BranchOrderingPolicy parseOrder( String order )
    {
        if ( order.equals( "depth first" ) || "depth first".startsWith( order.toLowerCase() ) )
        {
            return BranchOrderingPolicies.PREORDER_DEPTH_FIRST;
        }
        if ( order.equals( "breadth first" ) || "breadth first".startsWith( order.toLowerCase() ) )
        {
            return BranchOrderingPolicies.PREORDER_BREADTH_FIRST;
        }
        
        return parseEnum( CommonBranchOrdering.class, order, null );
    }
}
