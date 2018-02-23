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
package org.neo4j.kernel.impl.api;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Does test having multiple iterators open on the same index
 * <ul>
 * <li>Exhaust variations:</li>
 * <ul>
 * <li>Exhaust iterators one by one</li>
 * <li>Nesting</li>
 * <li>Interleaved</li>
 * </ul>
 * <li>Happy case for schema index iterators on static db for:</li>
 * <ul>
 * <li>Single property number index</li>
 * <li>Single property string index</li>
 * <li>Composite property number index</li>
 * <li>Composite property string index</li>
 * </ul>
 * <li>For index queries:</li>
 * <ul>
 * <li>{@link IndexQuery#exists(int)}</li>
 * <li>{@link IndexQuery#exact(int, Object)}</li>
 * <li>{@link IndexQuery#range(int, Number, boolean, Number, boolean)}</li>
 * <li>{@link IndexQuery#range(int, String, boolean, String, boolean)}</li>
 * </ul>
 * </ul>
 * Does NOT test
 * <ul>
 * <li>Single property unique number index</li>
 * <li>Single property unique string index</li>
 * <li>Composite property mixed index</li>
 * <li>{@link IndexQuery#stringPrefix(int, String)}</li>
 * <li>{@link IndexQuery#stringSuffix(int, String)}</li>
 * <li>{@link IndexQuery#stringContains(int, String)}</li>
 * <li>Composite property node key index (due to it being enterprise feature)</li>
 * <li>Label index iterators</li>
 * <li>Concurrency</li>
 * <li>Locking</li>
 * <li>Cluster</li>
 * <li>Index creation</li>
 * </ul>
 * Code navigation:
 */
@RunWith( Parameterized.class )
public class OperationsFacadeSchemaIndexIteratorTest
{
    private interface IndexCoordinatorFactory
    {
        IndexCoordinator create( Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2 );
    }

    private final DatabaseRule db = new EmbeddedDatabaseRule();

    private static final RandomRule rnd = new RandomRule();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( rnd ).around( db );

    private static final Label indexLabel = Label.label( "IndexLabel" );

    private static final String numberProp1 = "numberProp1";
    private static final String numberProp2 = "numberProp2";
    private static final String stringProp1 = "stringProp1";
    private static final String stringProp2 = "stringProp2";

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> params()
    {
        return Arrays.asList(
                new Object[]{"Single number non unique", (IndexCoordinatorFactory) NumberIndexCoordinator::new},
                new Object[]{"Single string non unique", (IndexCoordinatorFactory) StringIndexCoordinator::new},
                new Object[]{"Composite number non unique", (IndexCoordinatorFactory) NumberCompositeIndexCoordinator::new},
                new Object[]{"Composite string non unique", (IndexCoordinatorFactory) StringCompositeIndexCoordinator::new}
        );
    }

    @Parameterized.Parameter( 0 )
    public String name;

    @Parameterized.Parameter( 1 )
    public IndexCoordinatorFactory indexCoordinatorFactory;

    private IndexCoordinator indexCoordinator;

    @Before
    public void setupDb()
    {
        indexCoordinator = indexCoordinatorFactory.create( indexLabel, numberProp1, numberProp2, stringProp1, stringProp2 );
        indexCoordinator.init( db );
        indexCoordinator.createIndex( db );
    }

    @Test
    public void multipleIteratorsNotNestedExists() throws Exception
    {

        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryExists( readOperations );
            List<Long> actual1 = PrimitiveLongCollections.asList( iter1 );

            PrimitiveLongIterator iter2 = indexCoordinator.queryExists( readOperations );
            List<Long> actual2 = PrimitiveLongCollections.asList( iter2 );

            // then
            indexCoordinator.assertExistsResult( actual1 );
            indexCoordinator.assertExistsResult( actual2 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNotNestedExact() throws Exception
    {
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryExact( readOperations );
            List<Long> actual1 = PrimitiveLongCollections.asList( iter1 );

            PrimitiveLongIterator iter2 = indexCoordinator.queryExact( readOperations );
            List<Long> actual2 = PrimitiveLongCollections.asList( iter2 );

            // then
            indexCoordinator.assertExactResult( actual1 );
            indexCoordinator.assertExactResult( actual2 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNotNestedRange() throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        Assume.assumeTrue( indexCoordinator.supportRangeQuery() );
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryRange( readOperations );
            List<Long> actual1 = PrimitiveLongCollections.asList( iter1 );

            PrimitiveLongIterator iter2 = indexCoordinator.queryRange( readOperations );
            List<Long> actual2 = PrimitiveLongCollections.asList( iter2 );

            // then
            indexCoordinator.assertRangeResult( actual1 );
            indexCoordinator.assertRangeResult( actual2 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNestedInnerNewExists() throws Exception
    {
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryExists( readOperations );
            List<Long> actual1 = new ArrayList<>();
            while ( iter1.hasNext() )
            {
                actual1.add( iter1.next() );
                PrimitiveLongIterator iter2 = indexCoordinator.queryExists( readOperations );
                List<Long> actual2 = PrimitiveLongCollections.asList( iter2 );
                indexCoordinator.assertExistsResult( actual2 );
            }
            // then
            indexCoordinator.assertExistsResult( actual1 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNestedInnerNewExact() throws Exception
    {
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryExact( readOperations );
            List<Long> actual1 = new ArrayList<>();
            while ( iter1.hasNext() )
            {
                actual1.add( iter1.next() );
                PrimitiveLongIterator iter2 = indexCoordinator.queryExact( readOperations );
                List<Long> actual2 = PrimitiveLongCollections.asList( iter2 );
                indexCoordinator.assertExactResult( actual2 );
            }
            // then
            indexCoordinator.assertExactResult( actual1 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNestedInnerNewRange() throws Exception
    {
        Assume.assumeTrue( indexCoordinator.supportRangeQuery() );
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryRange( readOperations );
            List<Long> actual1 = new ArrayList<>();
            while ( iter1.hasNext() )
            {
                actual1.add( iter1.next() );
                PrimitiveLongIterator iter2 = indexCoordinator.queryRange( readOperations );
                List<Long> actual2 = PrimitiveLongCollections.asList( iter2 );
                indexCoordinator.assertRangeResult( actual2 );
            }
            // then
            indexCoordinator.assertRangeResult( actual1 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNestedInterleavedExists() throws Exception
    {
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryExists( readOperations );
            List<Long> actual1 = new ArrayList<>();
            PrimitiveLongIterator iter2 = indexCoordinator.queryExists( readOperations );
            List<Long> actual2 = new ArrayList<>();

            // Interleave
            exhaustInterleaved( iter1, actual1, iter2, actual2 );

            // then
            indexCoordinator.assertExistsResult( actual1 );
            indexCoordinator.assertExistsResult( actual2 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNestedInterleavedExact() throws Exception
    {
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryExact( readOperations );
            List<Long> actual1 = new ArrayList<>();
            PrimitiveLongIterator iter2 = indexCoordinator.queryExact( readOperations );
            List<Long> actual2 = new ArrayList<>();

            // Interleave
            exhaustInterleaved( iter1, actual1, iter2, actual2 );

            // then
            indexCoordinator.assertExactResult( actual1 );
            indexCoordinator.assertExactResult( actual2 );
            tx.success();
        }
    }

    @Test
    public void multipleIteratorsNestedInterleavedRange() throws Exception
    {
        Assume.assumeTrue( indexCoordinator.supportRangeQuery() );
        try ( Transaction tx = db.beginTx();
                Statement statement = db.statement() )
        {
            // when
            ReadOperations readOperations = statement.readOperations();
            PrimitiveLongIterator iter1 = indexCoordinator.queryRange( readOperations );
            List<Long> actual1 = new ArrayList<>();
            PrimitiveLongIterator iter2 = indexCoordinator.queryRange( readOperations );
            List<Long> actual2 = new ArrayList<>();

            // Interleave
            exhaustInterleaved( iter1, actual1, iter2, actual2 );

            // then
            indexCoordinator.assertRangeResult( actual1 );
            indexCoordinator.assertRangeResult( actual2 );
            tx.success();
        }
    }

    private void exhaustInterleaved( PrimitiveLongIterator source1, List<Long> target1, PrimitiveLongIterator source2, List<Long> target2 )
    {
        while ( source1.hasNext() && source2.hasNext() )
        {
            if ( rnd.nextBoolean() )
            {
                target1.add( source1.next() );
            }
            else
            {
                target2.add( source2.next() );
            }
        }

        // Empty the rest
        while ( source1.hasNext() )
        {
            target1.add( source1.next() );
        }
        while ( source2.hasNext() )
        {
            target2.add( source2.next() );
        }
    }

    private static class StringCompositeIndexCoordinator extends IndexCoordinator
    {
        StringCompositeIndexCoordinator( Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2 )
        {
            super( indexLabel, numberProp1, numberProp2, stringProp1, stringProp2 );
        }

        @Override
        protected IndexDescriptor extractIndexDescriptor()
        {
            return IndexDescriptorFactory.forLabel( indexedLabelId, stringPropId1, stringPropId2 );
        }

        @Override
        boolean supportRangeQuery()
        {
            return false;
        }

        @Override
        PrimitiveLongIterator queryRange( ReadOperations readOperations )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        PrimitiveLongIterator queryExists( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor,
                    IndexQuery.exists( stringPropId1 ),
                    IndexQuery.exists( stringPropId2 ) );
        }

        @Override
        PrimitiveLongIterator queryExact( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor,
                    IndexQuery.exact( stringPropId1, stringProp1Values[0] ),
                    IndexQuery.exact( stringPropId2, stringProp2Values[0] ) );
        }

        @Override
        void assertRangeResult( List<Long> result )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void assertExactResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            expected.add( 0L );
            assertSameContent( actual, expected );
        }

        @Override
        void doCreateIndex( DatabaseRule db )
        {
            db.schema().indexFor( indexLabel ).on( stringProp1 ).on( stringProp2 ).create();
        }
    }

    private static class NumberCompositeIndexCoordinator extends IndexCoordinator
    {
        NumberCompositeIndexCoordinator( Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2 )
        {
            super( indexLabel, numberProp1, numberProp2, stringProp1, stringProp2 );
        }

        @Override
        protected IndexDescriptor extractIndexDescriptor()
        {
            return IndexDescriptorFactory.forLabel( indexedLabelId, numberPropId1, numberPropId2 );
        }

        @Override
        boolean supportRangeQuery()
        {
            return false;
        }

        @Override
        PrimitiveLongIterator queryRange( ReadOperations readOperations )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        PrimitiveLongIterator queryExists( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor,
                    IndexQuery.exists( numberPropId1 ),
                    IndexQuery.exists( numberPropId2 ) );
        }

        @Override
        PrimitiveLongIterator queryExact( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor,
                    IndexQuery.exact( numberPropId1, numberProp1Values[0] ),
                    IndexQuery.exact( numberPropId2, numberProp2Values[0] ) );
        }

        @Override
        void assertRangeResult( List<Long> actual )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void assertExactResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            expected.add( 0L );
            assertSameContent( actual, expected );
        }

        @Override
        void doCreateIndex( DatabaseRule db )
        {
            db.schema().indexFor( indexLabel ).on( numberProp1 ).on( numberProp2 ).create();
        }
    }

    private static class StringIndexCoordinator extends IndexCoordinator
    {
        StringIndexCoordinator( Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2 )
        {
            super( indexLabel, numberProp1, numberProp2, stringProp1, stringProp2 );
        }

        @Override
        protected IndexDescriptor extractIndexDescriptor()
        {
            return IndexDescriptorFactory.forLabel( indexedLabelId, stringPropId1 );
        }

        @Override
        boolean supportRangeQuery()
        {
            return true;
        }

        @Override
        PrimitiveLongIterator queryRange( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            // query for half the range
            return readOperations.indexQuery( indexDescriptor,
                    IndexQuery.range( numberPropId1, stringProp1Values[0], true, stringProp1Values[numberOfNodes / 2], false ) );
        }

        @Override
        PrimitiveLongIterator queryExists( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor, IndexQuery.exists( stringPropId1 ) );
        }

        @Override
        PrimitiveLongIterator queryExact( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor, IndexQuery.exact( stringPropId1, stringProp1Values[0] ) );
        }

        @Override
        void assertRangeResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            for ( long i = 0; i < numberOfNodes / 2; i++ )
            {
                expected.add( i );
            }
            assertSameContent( actual, expected );
        }

        @Override
        void assertExactResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            expected.add( 0L );
            assertSameContent( actual, expected );
        }

        @Override
        void doCreateIndex( DatabaseRule db )
        {
            db.schema().indexFor( indexLabel ).on( stringProp1 ).create();
        }
    }

    private static class NumberIndexCoordinator extends IndexCoordinator
    {
        NumberIndexCoordinator( Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2 )
        {
            super( indexLabel, numberProp1, numberProp2, stringProp1, stringProp2 );
        }

        @Override
        protected IndexDescriptor extractIndexDescriptor()
        {
            return IndexDescriptorFactory.forLabel( indexedLabelId, numberPropId1 );
        }

        @Override
        boolean supportRangeQuery()
        {
            return true;
        }

        @Override
        PrimitiveLongIterator queryRange( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            // query for half the range
            return readOperations.indexQuery( indexDescriptor,
                    IndexQuery.range( numberPropId1, numberProp1Values[0], true, numberProp1Values[numberOfNodes / 2], false ) );
        }

        @Override
        PrimitiveLongIterator queryExists( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor, IndexQuery.exists( numberPropId1 ) );
        }

        @Override
        PrimitiveLongIterator queryExact( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException
        {
            return readOperations.indexQuery( indexDescriptor, IndexQuery.exact( numberPropId1, numberProp1Values[0] ) );
        }

        @Override
        void assertRangeResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            for ( long i = 0; i < numberOfNodes / 2; i++ )
            {
                expected.add( i );
            }
            assertSameContent( actual, expected );
        }

        @Override
        void assertExactResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            expected.add( 0L );
            assertSameContent( actual, expected );
        }

        @Override
        void doCreateIndex( DatabaseRule db )
        {
            db.schema().indexFor( indexLabel ).on( numberProp1 ).create();
        }
    }

    private abstract static class IndexCoordinator
    {
        final int numberOfNodes = 100;

        final Label indexLabel;
        final String numberProp1;
        final String numberProp2;
        final String stringProp1;
        final String stringProp2;

        Number[] numberProp1Values;
        Number[] numberProp2Values;
        String[] stringProp1Values;
        String[] stringProp2Values;

        int indexedLabelId;
        int numberPropId1;
        int numberPropId2;
        int stringPropId1;
        int stringPropId2;
        IndexDescriptor indexDescriptor;

        IndexCoordinator( Label indexLabel, String numberProp1, String numberProp2, String stringProp1, String stringProp2 )
        {
            this.indexLabel = indexLabel;
            this.numberProp1 = numberProp1;
            this.numberProp2 = numberProp2;
            this.stringProp1 = stringProp1;
            this.stringProp2 = stringProp2;

            this.numberProp1Values = new Number[numberOfNodes];
            this.numberProp2Values = new Number[numberOfNodes];
            this.stringProp1Values = new String[numberOfNodes];
            this.stringProp2Values = new String[numberOfNodes];

            // EXISTING DATA:
            // 100 nodes with properties:
            // numberProp1: 0-99
            // numberProp2: 0-99
            // stringProp1: "string-0"-"string-99"
            // stringProp2: "string-0"-"string-99"
            for ( int i = 0; i < numberOfNodes; i++ )
            {
                numberProp1Values[i] = i;
                numberProp2Values[i] = i;
                stringProp1Values[i] = "string-" + String.format( "%02d", i );
                stringProp2Values[i] = "string-" + String.format( "%02d", i );
            }
        }

        void init( DatabaseRule db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < numberOfNodes; i++ )
                {
                    Node node = db.createNode( indexLabel );
                    node.setProperty( numberProp1, numberProp1Values[i] );
                    node.setProperty( numberProp2, numberProp2Values[i] );
                    node.setProperty( stringProp1, stringProp1Values[i] );
                    node.setProperty( stringProp2, stringProp2Values[i] );
                }
                tx.success();
            }

            try ( Transaction tx = db.beginTx();
                    Statement statement = db.statement() )
            {
                ReadOperations readOp = statement.readOperations();
                indexedLabelId = readOp.labelGetForName( indexLabel.name() );
                numberPropId1 = readOp.propertyKeyGetForName( numberProp1 );
                numberPropId2 = readOp.propertyKeyGetForName( numberProp2 );
                stringPropId1 = readOp.propertyKeyGetForName( stringProp1 );
                stringPropId2 = readOp.propertyKeyGetForName( stringProp2 );
                tx.success();
            }
            indexDescriptor = extractIndexDescriptor();
        }

        protected abstract IndexDescriptor extractIndexDescriptor();

        void createIndex( DatabaseRule db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                doCreateIndex( db );
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                tx.success();
            }
        }

        abstract boolean supportRangeQuery();

        abstract PrimitiveLongIterator queryRange( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException;

        abstract PrimitiveLongIterator queryExists( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException;

        abstract PrimitiveLongIterator queryExact( ReadOperations readOperations )
                throws IndexNotApplicableKernelException, IndexNotFoundKernelException;

        abstract void assertRangeResult( List<Long> result );

        void assertExistsResult( List<Long> actual )
        {
            List<Long> expected = new ArrayList<>();
            for ( long i = 0; i < numberOfNodes; i++ )
            {
                expected.add( i );
            }
            assertSameContent( actual, expected );
        }

        void assertSameContent( List<Long> actual, List<Long> expected )
        {
            assertThat( actual, is( containsInAnyOrder( expected.toArray() ) ) );
        }

        abstract void assertExactResult( List<Long> result );

        abstract void doCreateIndex( DatabaseRule db );
    }
}
