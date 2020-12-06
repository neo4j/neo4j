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
package org.neo4j.shell.prettyprint;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.ListBoltResult;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Iterables.map;
import static org.neo4j.shell.prettyprint.OutputFormatter.NEWLINE;

@SuppressWarnings( "ArraysAsListWithZeroOrOneArgument" )
public class PrettyPrinterTest
{

    private final PrettyPrinter plainPrinter = new PrettyPrinter( new PrettyConfig( Format.PLAIN, false, 100 ) );
    private final PrettyPrinter verbosePrinter = new PrettyPrinter( new PrettyConfig( Format.VERBOSE, true, 100 ) );

    @Test
    public void returnStatisticsForEmptyRecords()
    {
        // given
        ResultSummary resultSummary = mock( ResultSummary.class );
        SummaryCounters summaryCounters = mock( SummaryCounters.class );
        BoltResult result = new ListBoltResult( Collections.emptyList(), resultSummary );

        when( resultSummary.counters() ).thenReturn( summaryCounters );
        when( summaryCounters.labelsAdded() ).thenReturn( 1 );
        when( summaryCounters.nodesCreated() ).thenReturn( 10 );

        // when
        String actual = verbosePrinter.format( result );

        // then
        assertThat( actual, containsString( "Added 10 nodes, Added 1 labels" ) );
    }

    @Test
    public void prettyPrintProfileInformation()
    {
        // given
        ResultSummary resultSummary = mock( ResultSummary.class );
        ProfiledPlan plan = mock( ProfiledPlan.class );
        when( plan.dbHits() ).thenReturn( 1000L );
        when( plan.records() ).thenReturn( 20L );

        when( resultSummary.hasPlan() ).thenReturn( true );
        when( resultSummary.hasProfile() ).thenReturn( true );
        when( resultSummary.plan() ).thenReturn( plan );
        when( resultSummary.profile() ).thenReturn( plan );
        when( resultSummary.resultAvailableAfter( anyObject() ) ).thenReturn( 5L );
        when( resultSummary.resultConsumedAfter( anyObject() ) ).thenReturn( 7L );
        when( resultSummary.queryType() ).thenReturn( QueryType.READ_ONLY );
        Map<String, Value> argumentMap = Values.parameters( "Version", "3.1", "Planner", "COST", "Runtime", "INTERPRETED" ).asMap( v -> v );
        when( plan.arguments() ).thenReturn( argumentMap );

        BoltResult result = new ListBoltResult( Collections.emptyList(), resultSummary );

        // when
        String actual = plainPrinter.format( result );

        // then
        String expected =
                "Plan: \"PROFILE\"\n" +
                "Statement: \"READ_ONLY\"\n" +
                "Version: \"3.1\"\n" +
                "Planner: \"COST\"\n" +
                "Runtime: \"INTERPRETED\"\n" +
                "Time: 12\n" +
                "Rows: 20\n" +
                "DbHits: 1000";
        Stream.of( expected.split( "\n" ) ).forEach( e -> assertThat( actual, containsString( e ) ) );
    }

    @Test
    public void prettyPrintExplainInformation()
    {
        // given
        ResultSummary resultSummary = mock( ResultSummary.class );
        ProfiledPlan plan = mock( ProfiledPlan.class );
        when( plan.dbHits() ).thenReturn( 1000L );
        when( plan.records() ).thenReturn( 20L );

        when( resultSummary.hasPlan() ).thenReturn( true );
        when( resultSummary.hasProfile() ).thenReturn( false );
        when( resultSummary.plan() ).thenReturn( plan );
        when( resultSummary.resultAvailableAfter( anyObject() ) ).thenReturn( 5L );
        when( resultSummary.resultConsumedAfter( anyObject() ) ).thenReturn( 7L );
        when( resultSummary.queryType() ).thenReturn( QueryType.READ_ONLY );
        Map<String, Value> argumentMap = Values.parameters( "Version", "3.1", "Planner", "COST", "Runtime", "INTERPRETED" ).asMap( v -> v );
        when( plan.arguments() ).thenReturn( argumentMap );

        BoltResult result = new ListBoltResult( Collections.emptyList(), resultSummary );

        // when
        String actual = plainPrinter.format( result );

        // then
        String expected =
                "Plan: \"EXPLAIN\"\n" +
                "Statement: \"READ_ONLY\"\n" +
                "Version: \"3.1\"\n" +
                "Planner: \"COST\"\n" +
                "Runtime: \"INTERPRETED\"\n" +
                "Time: 12";
        Stream.of( expected.split( "\n" ) ).forEach( e -> assertThat( actual, containsString( e ) ) );
    }

    @Test
    public void prettyPrintList()
    {
        // given
        Record record1 = mock( Record.class );
        Record record2 = mock( Record.class );
        Value value1 = Values.value( "val1_1", "val1_2" );
        Value value2 = Values.value( new String[] {"val2_1"} );

        when( record1.keys() ).thenReturn( asList( "col1", "col2" ) );
        when( record1.values() ).thenReturn( asList( value1, value2 ) );
        when( record2.values() ).thenReturn( asList( value2 ) );

        BoltResult result = new ListBoltResult( asList( record1, record2 ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( String.join( NEWLINE,
                                             "col1, col2",
                                             "[\"val1_1\", \"val1_2\"], [\"val2_1\"]",
                                             "[\"val2_1\"]",
                                             "" ) ) );
    }

    @Test
    public void prettyPrintMaps()
    {
        checkMapForPrettyPrint( map(), "map" + NEWLINE + "{}" + NEWLINE );
        checkMapForPrettyPrint( map( "abc", "def" ), "map" + NEWLINE + "{abc: def}" + NEWLINE );
    }

    private void checkMapForPrettyPrint( Map<String, String> map, String expectedResult )
    {
        // given
        Record record = mock( Record.class );
        Value value = mock( Value.class );

        when( value.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.MAP() );

        when( value.asMap( (Function<Value, String>) anyObject() ) ).thenReturn( map );

        when( record.keys() ).thenReturn( asList( "map" ) );
        when( record.values() ).thenReturn( asList( value ) );

        BoltResult result = new ListBoltResult( asList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( expectedResult ) );
    }

    @Test
    public void prettyPrintNode()
    {
        // given
        Record record = mock( Record.class );
        Value value = mock( Value.class );

        Node node = mock( Node.class );
        HashMap<String, Object> propertiesAsMap = new HashMap<>();
        propertiesAsMap.put( "prop1", "prop1_value" );
        propertiesAsMap.put( "prop2", "prop2_value" );

        when( value.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.NODE() );

        when( value.asNode() ).thenReturn( node );
        when( node.labels() ).thenReturn( asList( "label1", "label2" ) );
        when( node.asMap( anyObject() ) ).thenReturn( unmodifiableMap( propertiesAsMap ) );

        when( record.keys() ).thenReturn( asList( "col1", "col2" ) );
        when( record.values() ).thenReturn( asList( value ) );

        BoltResult result = new ListBoltResult( asList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( "col1, col2" + NEWLINE +
                                "(:label1:label2 {prop2: prop2_value, prop1: prop1_value})" + NEWLINE ) );
    }

    @Test
    public void prettyPrintRelationships()
    {
        // given
        Record record = mock( Record.class );
        Value value = mock( Value.class );

        Relationship relationship = mock( Relationship.class );
        HashMap<String, Object> propertiesAsMap = new HashMap<>();
        propertiesAsMap.put( "prop1", "prop1_value" );
        propertiesAsMap.put( "prop2", "prop2_value" );

        when( value.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP() );

        when( value.asRelationship() ).thenReturn( relationship );
        when( relationship.type() ).thenReturn( "RELATIONSHIP_TYPE" );
        when( relationship.asMap( anyObject() ) ).thenReturn( unmodifiableMap( propertiesAsMap ) );

        when( record.keys() ).thenReturn( asList( "rel" ) );
        when( record.values() ).thenReturn( asList( value ) );

        BoltResult result = new ListBoltResult( asList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( "rel" + NEWLINE +
                                "[:RELATIONSHIP_TYPE {prop2: prop2_value, prop1: prop1_value}]" + NEWLINE ) );
    }

    @Test
    public void printRelationshipsAndNodesWithEscapingForSpecialCharacters()
    {
        // given
        Record record = mock( Record.class );
        Value relVal = mock( Value.class );
        Value nodeVal = mock( Value.class );

        Relationship relationship = mock( Relationship.class );
        HashMap<String, Object> relProp = new HashMap<>();
        relProp.put( "prop1", "\"prop1, value\"" );
        relProp.put( "prop2", "prop2_value" );

        Node node = mock( Node.class );
        HashMap<String, Object> nodeProp = new HashMap<>();
        nodeProp.put( "prop1", "\"prop1:value\"" );
        nodeProp.put( "1prop2", "\"\"" );
        nodeProp.put( "ä", "not-escaped" );

        when( relVal.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.RELATIONSHIP() );
        when( nodeVal.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.NODE() );

        when( relVal.asRelationship() ).thenReturn( relationship );
        when( relationship.type() ).thenReturn( "RELATIONSHIP,TYPE" );
        when( relationship.asMap( anyObject() ) ).thenReturn( unmodifiableMap( relProp ) );

        when( nodeVal.asNode() ).thenReturn( node );
        when( node.labels() ).thenReturn( asList( "label `1", "label2" ) );
        when( node.asMap( anyObject() ) ).thenReturn( unmodifiableMap( nodeProp ) );

        when( record.keys() ).thenReturn( asList( "rel", "node" ) );
        when( record.values() ).thenReturn( asList( relVal, nodeVal ) );

        BoltResult result = new ListBoltResult( asList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is(
                "rel, node" + NEWLINE +
                "[:`RELATIONSHIP,TYPE` {prop2: prop2_value, prop1: \"prop1, value\"}], " +
                "(:`label ``1`:label2 {prop1: \"prop1:value\", `1prop2`: \"\", ä: not-escaped})" + NEWLINE ) );
    }

    @Test
    public void prettyPrintPaths()
    {
        // given
        Record record = mock( Record.class );
        Value value = mock( Value.class );

        Node start = mock( Node.class );
        HashMap<String, Object> startProperties = new HashMap<>();
        startProperties.put( "prop1", "prop1_value" );
        when( start.labels() ).thenReturn( asList( "start" ) );
        when( start.id() ).thenReturn( 1L );

        Node middle = mock( Node.class );
        when( middle.labels() ).thenReturn( asList( "middle" ) );
        when( middle.id() ).thenReturn( 2L );

        Node end = mock( Node.class );
        HashMap<String, Object> endProperties = new HashMap<>();
        endProperties.put( "prop2", "prop2_value" );
        when( end.labels() ).thenReturn( asList( "end" ) );
        when( end.id() ).thenReturn( 3L );

        Path path = mock( Path.class );
        when( path.start() ).thenReturn( start );

        Relationship relationship = mock( Relationship.class );
        when( relationship.type() ).thenReturn( "RELATIONSHIP_TYPE" );
        when( relationship.startNodeId() ).thenReturn( 1L ).thenReturn( 3L );

        Path.Segment segment1 = mock( Path.Segment.class );
        when( segment1.start() ).thenReturn( start );
        when( segment1.end() ).thenReturn( middle );
        when( segment1.relationship() ).thenReturn( relationship );

        Path.Segment segment2 = mock( Path.Segment.class );
        when( segment2.start() ).thenReturn( middle );
        when( segment2.end() ).thenReturn( end );
        when( segment2.relationship() ).thenReturn( relationship );

        when( value.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.PATH() );
        when( value.asPath() ).thenReturn( path );
        when( path.iterator() ).thenReturn( asList( segment1, segment2 ).iterator() );
        when( start.asMap( anyObject() ) ).thenReturn( unmodifiableMap( startProperties ) );
        when( end.asMap( anyObject() ) ).thenReturn( unmodifiableMap( endProperties ) );

        when( record.keys() ).thenReturn( asList( "path" ) );
        when( record.values() ).thenReturn( asList( value ) );

        BoltResult result = new ListBoltResult( asList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( "path" + NEWLINE +
                                "(:start {prop1: prop1_value})-[:RELATIONSHIP_TYPE]->" +
                                "(:middle)<-[:RELATIONSHIP_TYPE]-(:end {prop2: prop2_value})" + NEWLINE ) );
    }

    @Test
    public void prettyPrintSingleNodePath()
    {
        // given
        Record record = mock( Record.class );
        Value value = mock( Value.class );

        Node start = mock( Node.class );
        when( start.labels() ).thenReturn( asList( "start" ) );
        when( start.id() ).thenReturn( 1L );

        Node end = mock( Node.class );
        when( end.labels() ).thenReturn( asList( "end" ) );
        when( end.id() ).thenReturn( 2L );

        Path path = mock( Path.class );
        when( path.start() ).thenReturn( start );

        Relationship relationship = mock( Relationship.class );
        when( relationship.type() ).thenReturn( "RELATIONSHIP_TYPE" );
        when( relationship.startNodeId() ).thenReturn( 1L );

        Path.Segment segment1 = mock( Path.Segment.class );
        when( segment1.start() ).thenReturn( start );
        when( segment1.end() ).thenReturn( end );
        when( segment1.relationship() ).thenReturn( relationship );

        when( value.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.PATH() );
        when( value.asPath() ).thenReturn( path );
        when( path.iterator() ).thenReturn( asList( segment1 ).iterator() );

        when( record.keys() ).thenReturn( asList( "path" ) );
        when( record.values() ).thenReturn( asList( value ) );

        BoltResult result = new ListBoltResult( asList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( "path" + NEWLINE + "(:start)-[:RELATIONSHIP_TYPE]->(:end)" + NEWLINE ) );
    }

    @Test
    public void prettyPrintThreeSegmentPath()
    {
        // given
        Record record = mock( Record.class );
        Value value = mock( Value.class );

        Node start = mock( Node.class );
        when( start.labels() ).thenReturn( asList( "start" ) );
        when( start.id() ).thenReturn( 1L );

        Node second = mock( Node.class );
        when( second.labels() ).thenReturn( asList( "second" ) );
        when( second.id() ).thenReturn( 2L );

        Node third = mock( Node.class );
        when( third.labels() ).thenReturn( asList( "third" ) );
        when( third.id() ).thenReturn( 3L );

        Node end = mock( Node.class );
        when( end.labels() ).thenReturn( asList( "end" ) );
        when( end.id() ).thenReturn( 4L );

        Path path = mock( Path.class );
        when( path.start() ).thenReturn( start );

        Relationship relationship = mock( Relationship.class );
        when( relationship.type() ).thenReturn( "RELATIONSHIP_TYPE" );
        when( relationship.startNodeId() ).thenReturn( 1L ).thenReturn( 3L ).thenReturn( 3L );

        Path.Segment segment1 = mock( Path.Segment.class );
        when( segment1.start() ).thenReturn( start );
        when( segment1.end() ).thenReturn( second );
        when( segment1.relationship() ).thenReturn( relationship );

        Path.Segment segment2 = mock( Path.Segment.class );
        when( segment2.start() ).thenReturn( second );
        when( segment2.end() ).thenReturn( third );
        when( segment2.relationship() ).thenReturn( relationship );

        Path.Segment segment3 = mock( Path.Segment.class );
        when( segment3.start() ).thenReturn( third );
        when( segment3.end() ).thenReturn( end );
        when( segment3.relationship() ).thenReturn( relationship );

        when( value.type() ).thenReturn( InternalTypeSystem.TYPE_SYSTEM.PATH() );
        when( value.asPath() ).thenReturn( path );
        when( path.iterator() ).thenReturn( asList( segment1, segment2, segment3 ).iterator() );

        when( record.keys() ).thenReturn( asList( "path" ) );
        when( record.values() ).thenReturn( asList( value ) );

        BoltResult result = new ListBoltResult( singletonList( record ), mock( ResultSummary.class ) );

        // when
        String actual = plainPrinter.format( result );

        // then
        assertThat( actual, is( "path" + NEWLINE +
                                "(:start)-[:RELATIONSHIP_TYPE]->" +
                                "(:second)<-[:RELATIONSHIP_TYPE]-(:third)-[:RELATIONSHIP_TYPE]->(:end)" + NEWLINE ) );
    }
}
