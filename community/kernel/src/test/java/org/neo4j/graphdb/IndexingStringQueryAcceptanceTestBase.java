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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.test.mockito.matcher.Neo4jMatchers;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;

public abstract class IndexingStringQueryAcceptanceTestBase
{
    @ClassRule
    public static ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    @Rule
    public final TestName testName = new TestName();

    final String template;
    final String[] matching;
    final String[] nonMatching;
    final StringSearchMode searchMode;

    private Label LABEL;
    private String KEY = "name";
    private GraphDatabaseService db;

    protected IndexingStringQueryAcceptanceTestBase( String template, String[] matching,
            String[] nonMatching, StringSearchMode searchMode )
    {
        this.template = template;
        this.matching = matching;
        this.nonMatching = nonMatching;
        this.searchMode = searchMode;
    }

    @Before
    public void setupLabels()
    {
        LABEL = Label.label( "LABEL1-" + testName.getMethodName() );
        db = dbRule.getGraphDatabaseAPI();
        Neo4jMatchers.createIndex( db, LABEL, KEY );
    }

    @Test
    public void shouldSupportIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching );
        PrimitiveLongSet expected = createNodes( db, LABEL, matching );

        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            collectNodes( found, db.findNodes( LABEL, KEY, template, searchMode ) );
        }

        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldIncludeNodesCreatedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0], nonMatching[1] );
        PrimitiveLongSet expected = createNodes( db, LABEL, matching[0], matching[1] );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            expected.add( createNode( db, map( KEY, matching[2] ), LABEL ).getId() );
            createNode( db, map( KEY, nonMatching[2] ), LABEL );

            collectNodes( found, db.findNodes( LABEL, KEY, template, searchMode ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldNotIncludeNodesDeletedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0] );
        PrimitiveLongSet toDelete = createNodes( db, LABEL, matching[0], nonMatching[1], matching[1], nonMatching[2] );
        PrimitiveLongSet expected = createNodes( db, LABEL, matching[2] );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator deleting = toDelete.iterator();
            while ( deleting.hasNext() )
            {
                long id = deleting.next();
                db.getNodeById( id ).delete();
                expected.remove( id );
            }

            collectNodes( found, db.findNodes( LABEL, KEY, template, searchMode ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldConsiderNodesChangedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0] );
        PrimitiveLongSet toChangeToMatch = createNodes( db, LABEL, nonMatching[1] );
        PrimitiveLongSet toChangeToNotMatch = createNodes( db, LABEL, matching[0] );
        PrimitiveLongSet expected = createNodes( db, LABEL, matching[1] );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator toMatching = toChangeToMatch.iterator();
            while ( toMatching.hasNext() )
            {
                long id = toMatching.next();
                db.getNodeById( id ).setProperty( KEY, matching[2] );
                expected.add( id );
            }
            PrimitiveLongIterator toNotMatching = toChangeToNotMatch.iterator();
            while ( toNotMatching.hasNext() )
            {
                long id = toNotMatching.next();
                db.getNodeById( id ).setProperty( KEY, nonMatching[2] );
                expected.remove( id );
            }

            collectNodes( found, db.findNodes( LABEL, KEY, template, searchMode ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    public static class EXACT extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"Johan", "Johan", "Johan"};
        static String[] nonMatching = {"Johanna", "Olivia", "InteJohan"};

        public EXACT()
        {
            super( "Johan", matching, nonMatching, StringSearchMode.EXACT );
        }
    }

    public static class PREFIX extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"Olivia", "Olivia2", "OliviaYtterbrink"};
        static String[] nonMatching = {"Johan", "olivia", "InteOlivia"};

        public PREFIX()
        {
            super( "Olivia", matching, nonMatching, StringSearchMode.PREFIX );
        }
    }

    public static class SUFFIX extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"Jansson", "Hansson", "Svensson"};
        static String[] nonMatching = {"Taverner", "Svensson-Averbuch", "Taylor"};

        public SUFFIX()
        {
            super( "sson", matching, nonMatching, StringSearchMode.SUFFIX );
        }
    }

    public static class CONTAINS extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"good", "fool", "fooooood"};
        static String[] nonMatching = {"evil", "genius", "hungry"};

        public CONTAINS()
        {
            super( "oo", matching, nonMatching, StringSearchMode.CONTAINS );
        }
    }

    private PrimitiveLongSet createNodes( GraphDatabaseService db, Label label, String... propertyValues )
    {
        PrimitiveLongSet expected = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( String value : propertyValues )
            {
                expected.add( createNode( db, map( KEY, value ), label ).getId() );
            }
            tx.success();
        }
        return expected;
    }

    private void collectNodes( PrimitiveLongSet bucket, ResourceIterator<Node> toCollect )
    {
        while ( toCollect.hasNext() )
        {
            bucket.add( toCollect.next().getId() );
        }
    }

    private Node createNode( GraphDatabaseService beansAPI, Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = beansAPI.beginTx() )
        {
            Node node = beansAPI.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.success();
            return node;
        }
    }
}
