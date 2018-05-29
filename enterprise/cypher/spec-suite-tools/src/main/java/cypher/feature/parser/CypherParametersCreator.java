/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.feature.parser;

import org.opencypher.tools.tck.parsing.generated.FeatureResultsBaseListener;
import org.opencypher.tools.tck.parsing.generated.FeatureResultsParser;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

class CypherParametersCreator extends FeatureResultsBaseListener
{
    private Deque<Object> workload;
    private Deque<Integer> listCounters;
    private Deque<Integer> mapCounters;

    private static final String INFINITY = "Inf";

    CypherParametersCreator()
    {
        this.workload = new LinkedList<>();
        this.listCounters = new LinkedList<>();
        this.mapCounters = new LinkedList<>();
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
        // Using a Deque here in order to be able to prepend
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

}
