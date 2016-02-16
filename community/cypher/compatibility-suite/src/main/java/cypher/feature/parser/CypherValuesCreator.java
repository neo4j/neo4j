/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.parser;

import cypher.feature.parser.generated.FeatureResultsBaseListener;
import cypher.feature.parser.generated.FeatureResultsParser;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.neo4j.graphdb.Label;

class CypherValuesCreator extends FeatureResultsBaseListener
{
    private Stack<Object> workload;
    private Stack<Integer> listCounters;
    private Stack<Integer> mapCounters;
    private List<Label> labels;

    private static final String INFINITY = "Inf";

    CypherValuesCreator()
    {
        this.workload = new Stack<>();
        this.listCounters = new Stack<>();
        this.mapCounters = new Stack<>();
        this.labels = new ArrayList<>();
    }

    Object parsed()
    {
        return workload.pop();
    }

    @Override
    public void enterInteger( FeatureResultsParser.IntegerContext ctx )
    {
        workload.push( Long.valueOf( ctx.getText() ) );
    }

    @Override
    public void enterNullValue( FeatureResultsParser.NullValueContext ctx )
    {
        workload.push( null );
    }

    @Override
    public void enterFloatingPoint( FeatureResultsParser.FloatingPointContext ctx )
    {
        String text = ctx.getText();
        if ( text.contains( INFINITY ) )
        {
            workload.push( Double.parseDouble( text + "inity" ) );
        }
        else
        {
            workload.push( Double.parseDouble( text ) );
        }
    }

    @Override
    public void enterBool( FeatureResultsParser.BoolContext ctx )
    {
        workload.push( Boolean.valueOf( ctx.getText() ) );
    }

    @Override
    public void enterString( FeatureResultsParser.StringContext ctx )
    {
        String text = ctx.getText();
        String substring = text.substring( 1,
                text.length() - 1 ); // remove wrapping quotes -- because I can't get the parser rules correct :(
        String escaped = substring.replace( "\\'", "'" ); // remove escaping backslash -- see above comment
        workload.push( escaped );
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
        Deque<Object> temp = new LinkedList<>();
        int counter = listCounters.pop();
        for ( int i = 0; i < counter; ++i )
        {
            temp.addFirst( workload.pop() );
        }
        workload.push( temp );
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
        Map<String,Object> map = new HashMap<>();
        for ( int i = 0; i < counter; ++i )
        {
            Object value = workload.pop();
            String key = workload.pop().toString();
            map.put( key, value );
        }
        workload.push( map );
    }

    @Override
    public void enterPropertyKey( FeatureResultsParser.PropertyKeyContext ctx )
    {
        workload.push( ctx.getText() );
    }

    @Override
    public void enterLabelName( FeatureResultsParser.LabelNameContext ctx )
    {
        labels.add( Label.label( ctx.getText() ) );
    }

    @Override
    public void exitNode( FeatureResultsParser.NodeContext ctx )
    {
        final Map<String,Object> properties;
        if ( workload.empty() )
        {
            properties = new HashMap<>();
        }
        else
        {
            properties = (Map<String,Object>) workload.pop();
        }
        final ArrayList<Label> nodeLabels = new ArrayList<>( labels );
        labels.clear();
        workload.push( new ParsedNode()
        {
            @Override
            public Map<String,Object> getAllProperties()
            {
                return properties;
            }

            @Override
            public Iterable<Label> getLabels()
            {
                return nodeLabels;
            }
        } );
    }
}
