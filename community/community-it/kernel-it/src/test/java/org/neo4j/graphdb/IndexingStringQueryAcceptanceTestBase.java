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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@ImpermanentDbmsExtension
abstract class IndexingStringQueryAcceptanceTestBase
{
    private final String template;
    private final String[] matching;
    private final String[] nonMatching;
    private final StringSearchMode searchMode;
    private final boolean withIndex;

    private Label LABEL;
    private static final String KEY = "name";
    @Inject
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

    @BeforeEach
    void setup( TestInfo testInfo )
    {
        LABEL = Label.label( "LABEL1-" + testInfo.getDisplayName() );
        if ( withIndex )
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                tx.schema().indexFor( LABEL ).on( KEY ).create();
                tx.commit();
            }

            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
                tx.commit();
            }
        }
    }

    @Test
    void shouldSupportIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching );
        LongSet expected = createNodes( db, LABEL, matching );

        // WHEN
        MutableLongSet found = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            collectNodes( found, tx.findNodes( LABEL, KEY, template, searchMode ) );
        }

        // THEN
        assertThat( found ).isEqualTo( expected );
    }

    @Test
    void shouldIncludeNodesCreatedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0], nonMatching[1] );
        MutableLongSet expected = createNodes( db, LABEL, matching[0], matching[1] );
        // WHEN
        MutableLongSet found = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            expected.add( createNode( tx, map( KEY, matching[2] ), LABEL ).getId() );
            createNode( tx, map( KEY, nonMatching[2] ), LABEL );

            collectNodes( found, tx.findNodes( LABEL, KEY, template, searchMode ) );
        }
        // THEN
        assertThat( found ).isEqualTo( expected );
    }

    @Test
    void shouldNotIncludeNodesDeletedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0] );
        LongSet toDelete = createNodes( db, LABEL, matching[0], nonMatching[1], matching[1], nonMatching[2] );
        MutableLongSet expected = createNodes( db, LABEL, matching[2] );
        // WHEN
        MutableLongSet found = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            LongIterator deleting = toDelete.longIterator();
            while ( deleting.hasNext() )
            {
                long id = deleting.next();
                tx.getNodeById( id ).delete();
                expected.remove( id );
            }

            collectNodes( found, tx.findNodes( LABEL, KEY, template, searchMode ) );
        }
        // THEN
        assertThat( found ).isEqualTo( expected );
    }

    @Test
    void shouldConsiderNodesChangedInSameTxInIndexSeek()
    {
        // GIVEN
        createNodes( db, LABEL, nonMatching[0] );
        LongSet toChangeToMatch = createNodes( db, LABEL, nonMatching[1] );
        MutableLongSet toChangeToNotMatch = createNodes( db, LABEL, matching[0] );
        MutableLongSet expected = createNodes( db, LABEL, matching[1] );
        // WHEN
        MutableLongSet found = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            LongIterator toMatching = toChangeToMatch.longIterator();
            while ( toMatching.hasNext() )
            {
                long id = toMatching.next();
                tx.getNodeById( id ).setProperty( KEY, matching[2] );
                expected.add( id );
            }
            LongIterator toNotMatching = toChangeToNotMatch.longIterator();
            while ( toNotMatching.hasNext() )
            {
                long id = toNotMatching.next();
                tx.getNodeById( id ).setProperty( KEY, nonMatching[2] );
                expected.remove( id );
            }

            collectNodes( found, tx.findNodes( LABEL, KEY, template, searchMode ) );
        }
        // THEN
        assertThat( found ).isEqualTo( expected );
    }

    abstract static class ExactIndexingStringQueryAcceptanceTest extends IndexingStringQueryAcceptanceTestBase
    {
        static final String[] MATCHING = {"Johan", "Johan", "Johan"};
        static final String[] NON_MATCHING = {"Johanna", "Olivia", "InteJohan"};

        ExactIndexingStringQueryAcceptanceTest( boolean withIndex )
        {
            super( "Johan", MATCHING, NON_MATCHING, StringSearchMode.EXACT, withIndex );
        }
    }

    static class ExactWithIndexIndexingStringQueryAcceptanceTest extends ExactIndexingStringQueryAcceptanceTest
    {
        ExactWithIndexIndexingStringQueryAcceptanceTest()
        {
            super( true );
        }
    }

    static class ExactWithoutIndexIndexingStringQueryAcceptanceTest extends ExactIndexingStringQueryAcceptanceTest
    {
        ExactWithoutIndexIndexingStringQueryAcceptanceTest()
        {
            super( false );
        }
    }

    abstract static class PrefixIndexingStringQueryAcceptanceTest extends IndexingStringQueryAcceptanceTestBase
    {
        static final String[] MATCHING = {"Olivia", "Olivia2", "OliviaYtterbrink"};
        static final String[] NON_MATCHING = {"Johan", "olivia", "InteOlivia"};

        PrefixIndexingStringQueryAcceptanceTest( boolean withIndex )
        {
            super( "Olivia", MATCHING, NON_MATCHING, StringSearchMode.PREFIX, withIndex );
        }
    }

    static class PrefixWithIndexIndexingStringQueryAcceptanceTest extends PrefixIndexingStringQueryAcceptanceTest
    {
        PrefixWithIndexIndexingStringQueryAcceptanceTest()
        {
            super( true );
        }
    }

    static class PrefixWithoutIndexIndexingStringQueryAcceptanceTest extends PrefixIndexingStringQueryAcceptanceTest
    {
        PrefixWithoutIndexIndexingStringQueryAcceptanceTest()
        {
            super( false );
        }
    }

    abstract static class SuffixIndexingStringQueryAcceptanceTest extends IndexingStringQueryAcceptanceTestBase
    {
        static final String[] MATCHING = {"Jansson", "Hansson", "Svensson"};
        static final String[] NON_MATCHING = {"Taverner", "Svensson-Averbuch", "Taylor"};

        SuffixIndexingStringQueryAcceptanceTest( boolean withIndex )
        {
            super( "sson", MATCHING, NON_MATCHING, StringSearchMode.SUFFIX, withIndex );
        }
    }

    static class SuffixWithIndexIndexingStringQueryAcceptanceTest extends SuffixIndexingStringQueryAcceptanceTest
    {
        SuffixWithIndexIndexingStringQueryAcceptanceTest()
        {
            super( true );
        }
    }

    static class SuffixWithoutIndexIndexingStringQueryAcceptanceTest extends SuffixIndexingStringQueryAcceptanceTest
    {
        SuffixWithoutIndexIndexingStringQueryAcceptanceTest()
        {
            super( false );
        }
    }

    abstract static class ContainsIndexingStringQueryAcceptanceTest extends IndexingStringQueryAcceptanceTestBase
    {
        static final String[] MATCHING = {"good", "fool", "fooooood"};
        static final String[] NON_MATCHING = {"evil", "genius", "hungry"};

        ContainsIndexingStringQueryAcceptanceTest( boolean withIndex )
        {
            super( "oo", MATCHING, NON_MATCHING, StringSearchMode.CONTAINS, withIndex );
        }
    }

    static class ContainsWithIndexIndexingStringQueryAcceptanceTest extends ContainsIndexingStringQueryAcceptanceTest
    {
        ContainsWithIndexIndexingStringQueryAcceptanceTest()
        {
            super( true );
        }
    }

    static class ContainsWithoutIndexIndexingStringQueryAcceptanceTest extends ContainsIndexingStringQueryAcceptanceTest
    {
        ContainsWithoutIndexIndexingStringQueryAcceptanceTest()
        {
            super( false );
        }
    }

    private MutableLongSet createNodes( GraphDatabaseService db, Label label, String... propertyValues )
    {
        MutableLongSet expected = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( String value : propertyValues )
            {
                expected.add( createNode( tx, map( KEY, value ), label ).getId() );
            }
            tx.commit();
        }
        return expected;
    }

    private static void collectNodes( MutableLongSet bucket, ResourceIterator<Node> toCollect )
    {
        while ( toCollect.hasNext() )
        {
            bucket.add( toCollect.next().getId() );
        }
    }

    private static Node createNode( Transaction tx, Map<String,Object> properties, Label... labels )
    {
        Node node = tx.createNode( labels );
        for ( Map.Entry<String,Object> property : properties.entrySet() )
        {
            node.setProperty( property.getKey(), property.getValue() );
        }
        return node;
    }
}
