/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
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

    private final String template;
    private final String[] matching;
    private final String[] nonMatching;
    private final StringSearchMode searchMode;
    private final boolean withIndex;

    private Label LABEL;
    private String KEY = "name";
    private GraphDatabaseService db;

    IndexingStringQueryAcceptanceTestBase( String template, String[] matching,
            String[] nonMatching, StringSearchMode searchMode, boolean withIndex )
    {
        this.template = template;
        this.matching = matching;
        this.nonMatching = nonMatching;
        this.searchMode = searchMode;
        this.withIndex = withIndex;
    }

    @Before
    public void setup()
    {
        LABEL = Label.label( "LABEL1-" + testName.getMethodName() );
        db = dbRule.getGraphDatabaseAPI();
        if ( withIndex )
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( LABEL ).on( KEY ).create();
                tx.success();
            }

            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
                tx.success();
            }
        }
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

    public abstract static class EXACT extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"Johan", "Johan", "Johan"};
        static String[] nonMatching = {"Johanna", "Olivia", "InteJohan"};

        EXACT( boolean withIndex )
        {
            super( "Johan", matching, nonMatching, StringSearchMode.EXACT, withIndex );
        }
    }

    public static class EXACT_WITH_INDEX extends EXACT
    {
        public EXACT_WITH_INDEX()
        {
            super( true );
        }
    }

    public static class EXACT_WITHOUT_INDEX extends EXACT
    {
        public EXACT_WITHOUT_INDEX()
        {
            super( false );
        }
    }

    public abstract static class PREFIX extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"Olivia", "Olivia2", "OliviaYtterbrink"};
        static String[] nonMatching = {"Johan", "olivia", "InteOlivia"};

        PREFIX( boolean withIndex )
        {
            super( "Olivia", matching, nonMatching, StringSearchMode.PREFIX, withIndex );
        }
    }

    public static class PREFIX_WITH_INDEX extends PREFIX
    {
        public PREFIX_WITH_INDEX()
        {
            super( true );
        }
    }

    public static class PREFIX_WITHOUT_INDEX extends PREFIX
    {
        public PREFIX_WITHOUT_INDEX()
        {
            super( false );
        }
    }

    public abstract static class SUFFIX extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"Jansson", "Hansson", "Svensson"};
        static String[] nonMatching = {"Taverner", "Svensson-Averbuch", "Taylor"};

        SUFFIX( boolean withIndex )
        {
            super( "sson", matching, nonMatching, StringSearchMode.SUFFIX, withIndex );
        }
    }

    public static class SUFFIX_WITH_INDEX extends SUFFIX
    {
        public SUFFIX_WITH_INDEX()
        {
            super( true );
        }
    }

    public static class SUFFIX_WITHOUT_INDEX extends SUFFIX
    {
        public SUFFIX_WITHOUT_INDEX()
        {
            super( false );
        }
    }

    public abstract static class CONTAINS extends IndexingStringQueryAcceptanceTestBase
    {
        static String[] matching = {"good", "fool", "fooooood"};
        static String[] nonMatching = {"evil", "genius", "hungry"};

        public CONTAINS( boolean withIndex )
        {
            super( "oo", matching, nonMatching, StringSearchMode.CONTAINS, withIndex );
        }
    }

    public static class CONTAINS_WITH_INDEX extends CONTAINS
    {
        public CONTAINS_WITH_INDEX()
        {
            super( true );
        }
    }

    public static class CONTAINS_WITHOUT_INDEX extends CONTAINS
    {
        public CONTAINS_WITHOUT_INDEX()
        {
            super( false );
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
