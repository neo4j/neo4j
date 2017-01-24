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
package cypher.feature.parser;

import cypher.feature.parser.matchers.BooleanMatcher;
import cypher.feature.parser.matchers.FloatMatcher;
import cypher.feature.parser.matchers.IntegerMatcher;
import cypher.feature.parser.matchers.ListMatcher;
import cypher.feature.parser.matchers.MapMatcher;
import cypher.feature.parser.matchers.NodeMatcher;
import cypher.feature.parser.matchers.PathLinkMatcher;
import cypher.feature.parser.matchers.PathMatcher;
import cypher.feature.parser.matchers.RelationshipMatcher;
import cypher.feature.parser.matchers.StringMatcher;
import cypher.feature.parser.matchers.UnorderedListMatcher;
import cypher.feature.parser.matchers.ValueMatcher;
import org.opencypher.tools.tck.parsing.generated.FeatureResultsBaseListener;
import org.opencypher.tools.tck.parsing.generated.FeatureResultsParser;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

class CypherMatchersCreator extends FeatureResultsBaseListener
{
    private final Deque<ValueMatcher> workload;
    private final Deque<Integer> listCounters;
    private final Deque<Integer> mapCounters;
    private final Deque<String> keys;
    private final Deque<String> names;
    private final Deque<PathLinkMatcher> pathElements;

    private boolean unorderedLists;

    private static final String INFINITY = "Inf";

    CypherMatchersCreator()
    {
        this.workload = new LinkedList<>();
        this.keys = new LinkedList<>();
        this.listCounters = new LinkedList<>();
        this.mapCounters = new LinkedList<>();
        this.names = new LinkedList<>();
        this.pathElements = new LinkedList<>();
        this.unorderedLists = false;
    }

    ValueMatcher parsed()
    {
        return workload.pop();
    }

    CypherMatchersCreator setLists( boolean unordered )
    {
        unorderedLists = unordered;
        return this;
    }

    @Override
    public void enterInteger( FeatureResultsParser.IntegerContext ctx )
    {
        workload.push( new IntegerMatcher( Long.valueOf( ctx.getText() ) ) );
    }

    @Override
    public void enterNullValue( FeatureResultsParser.NullValueContext ctx )
    {
        workload.push( ValueMatcher.NULL_MATCHER );
    }

    @Override
    public void enterFloatingPoint( FeatureResultsParser.FloatingPointContext ctx )
    {
        String text = ctx.getText();
        if ( text.contains( INFINITY ) )
        {
            workload.push( new FloatMatcher( Double.parseDouble( text + "inity" ) ) );
        }
        else
        {
            workload.push( new FloatMatcher( Double.parseDouble( text ) ) );
        }
    }

    @Override
    public void enterBool( FeatureResultsParser.BoolContext ctx )
    {
        workload.push( new BooleanMatcher( Boolean.valueOf( ctx.getText() ) ) );
    }

    @Override
    public void enterString( FeatureResultsParser.StringContext ctx )
    {
        String text = ctx.getText();
        String substring = text.substring( 1,
                text.length() - 1 ); // remove wrapping quotes -- because I can't get the parser rules correct :(
        String escaped = substring.replace( "\\'", "'" ); // remove escaping backslash -- see above comment
        workload.push( new StringMatcher( escaped ) );
    }

    @Override
    public void enterList( FeatureResultsParser.ListContext ctx )
    {
        listCounters.push( 0 );
    }

    @Override
    public void enterListElement( FeatureResultsParser.ListElementContext ctx )
    {
        listCounters.push( listCounters.pop() + 1 );
    }

    @Override
    public void exitList( FeatureResultsParser.ListContext ctx )
    {
        // Using a LinkedList here in order to be able to prepend
        LinkedList<ValueMatcher> temp = new LinkedList<>();
        int counter = listCounters.pop();
        for ( int i = 0; i < counter; ++i )
        {
            temp.addFirst( workload.pop() );
        }
        if ( unorderedLists )
        {
            workload.push( new UnorderedListMatcher( temp ) );
        }
        else
        {
            workload.push( new ListMatcher( temp ) );
        }
    }

    @Override
    public void enterPropertyMap( FeatureResultsParser.PropertyMapContext ctx )
    {
        mapCounters.push( 0 );
    }

    @Override
    public void enterKeyValuePair( FeatureResultsParser.KeyValuePairContext ctx )
    {
        mapCounters.push( mapCounters.pop() + 1 );
    }

    @Override
    public void exitPropertyMap( FeatureResultsParser.PropertyMapContext ctx )
    {
        int counter = mapCounters.pop();
        Map<String,ValueMatcher> map = new HashMap<>();
        for ( int i = 0; i < counter; ++i )
        {
            ValueMatcher value = workload.pop();
            String key = keys.pop();
            map.put( key, value );
        }
        workload.push( new MapMatcher( map ) );
    }

    @Override
    public void enterPropertyKey( FeatureResultsParser.PropertyKeyContext ctx )
    {
        keys.push( ctx.getText() );
    }

    @Override
    public void enterLabelName( FeatureResultsParser.LabelNameContext ctx )
    {
        names.push( ctx.getText() );
    }

    @Override
    public void exitNodeDesc( FeatureResultsParser.NodeDescContext ctx )
    {
        MapMatcher properties = getMapMatcher();

        Set<String> labelNames = new HashSet<>();
        while ( !names.isEmpty() )
        {
            labelNames.add( names.pop() );
        }
        workload.push( new NodeMatcher( labelNames, properties ) );
    }

    private MapMatcher getMapMatcher()
    {
        if ( workload.peek() instanceof MapMatcher )
        {
            return (MapMatcher) workload.pop();
        }
        else
        {
            return MapMatcher.EMPTY;
        }
    }

    @Override
    public void enterRelationshipTypeName( FeatureResultsParser.RelationshipTypeNameContext ctx )
    {
        names.push( ctx.getText() );
    }

    @Override
    public void exitRelationshipDesc( FeatureResultsParser.RelationshipDescContext ctx )
    {
        MapMatcher properties = getMapMatcher();
        String relTypeName = names.pop();
        workload.push( new RelationshipMatcher( relTypeName, properties ) );
    }

    @Override
    public void exitForwardsRelationship( FeatureResultsParser.ForwardsRelationshipContext ctx )
    {
        constructPartialPathLink( true );
    }

    @Override
    public void exitBackwardsRelationship( FeatureResultsParser.BackwardsRelationshipContext ctx )
    {
        constructPartialPathLink( false );
    }

    private void constructPartialPathLink( boolean outgoing )
    {
        // On the workload there is a RelationshipMatcher
        RelationshipMatcher relMatcher = (RelationshipMatcher) workload.pop();

        // On the workload there is now a NodeMatcher, which is the start node for this relationship
        NodeMatcher startNode = (NodeMatcher) workload.pop();

        pathElements.push( new PathLinkMatcher( relMatcher, startNode, outgoing ) );
    }

    @Override
    public void exitPathLink( FeatureResultsParser.PathLinkContext ctx )
    {
        // On the workload there is a NodeMatcher, which is the end node of the last pathLink in pathElements

        NodeMatcher nodeMatcher = (NodeMatcher) workload.peek();
        pathElements.peek().setRightNode( nodeMatcher );
    }

    @Override
    public void exitPath( FeatureResultsParser.PathContext ctx )
    {
        // On the workload there is a NodeMatcher, and then zero or more (pathLength) PathLinkMatchers

        NodeMatcher singleNodePath = (NodeMatcher) workload.pop();
        if ( pathElements.isEmpty() )
        {
            workload.push( new PathMatcher( singleNodePath ) );
        }
        else
        {
            ArrayList<PathLinkMatcher> pathLinkMatchers = new ArrayList<>();
            pathElements.descendingIterator().forEachRemaining( pathLinkMatchers::add );
            pathElements.clear();
            workload.push( new PathMatcher( pathLinkMatchers ) );
        }
    }
}
