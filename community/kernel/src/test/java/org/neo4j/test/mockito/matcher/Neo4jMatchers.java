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
package org.neo4j.test.mockito.matcher;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.helpers.collection.Iterables.map;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.loop;

public class Neo4jMatchers
{
    private Neo4jMatchers()
    {
    }

    public static <T> Matcher<? super T> inTx( final GraphDatabaseService db, final Matcher<T> inner )
    {
        return inTx( db, inner, false );
    }

    public static <T> Matcher<? super T> inTx( final GraphDatabaseService db, final Matcher<T> inner,
            final boolean successful )
    {
        return new DiagnosingMatcher<>()
        {
            @Override
            protected boolean matches( Object item, Description mismatchDescription )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    if ( inner instanceof TransactionalMatcher )
                    {
                        ((TransactionalMatcher) inner).setTransaction( tx );
                    }
                    if ( inner.matches( item ) )
                    {
                        if ( successful )
                        {
                            tx.commit();
                        }
                        return true;
                    }

                    inner.describeMismatch( item, mismatchDescription );

                    if ( successful )
                    {
                        tx.commit();
                    }
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                inner.describeTo( description );
            }
        };
    }

    public static TypeSafeDiagnosingMatcher<Node> hasLabel( final Label myLabel )
    {
        return new LabelMatcher( myLabel );
    }

    public static TypeSafeDiagnosingMatcher<Node> hasLabels( String... expectedLabels )
    {
        return hasLabels( asSet( expectedLabels ) );
    }

    public static TypeSafeDiagnosingMatcher<Node> hasLabels( Label... expectedLabels )
    {
        Set<String> labelNames = new HashSet<>( expectedLabels.length );
        for ( Label l : expectedLabels )
        {
            labelNames.add( l.name() );
        }
        return hasLabels( labelNames );
    }

    public static TypeSafeDiagnosingMatcher<Node> hasNoLabels()
    {
        return hasLabels( emptySet() );
    }

    public static TypeSafeDiagnosingMatcher<Node> hasLabels( final Set<String> expectedLabels )
    {
        return new LabelsMatcher( expectedLabels );
    }

    public static TypeSafeDiagnosingMatcher<Transaction> hasNoNodes( final Label withLabel )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Transaction tx, Description mismatchDescription )
            {
                Set<Node> found = asSet( tx.findNodes( withLabel ) );
                if ( !found.isEmpty() )
                {
                    mismatchDescription.appendText( "found " + found.toString() );
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "no nodes with label " + withLabel );
            }
        };
    }

    public static TypeSafeDiagnosingMatcher<Transaction> hasNodes( final Label withLabel, final Node... expectedNodes )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Transaction tx, Description mismatchDescription )
            {
                Set<Node> expected = asSet( expectedNodes );
                Set<Node> found = asSet( tx.findNodes( withLabel ) );
                if ( !expected.equals( found ) )
                {
                    mismatchDescription.appendText( "found " + found.toString() );
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( asSet( expectedNodes ).toString() + " with label " + withLabel );
            }
        };
    }

    public static Set<String> asLabelNameSet( Iterable<Label> enums )
    {
        return Iterables.asSet( map( Label::name, enums ) );
    }

    public static class PropertyValueMatcher extends TypeSafeDiagnosingMatcher<Entity> implements TransactionalMatcher
    {
        private final PropertyMatcher propertyMatcher;
        private final String propertyName;
        private final Object expectedValue;
        private Transaction transaction;

        private PropertyValueMatcher( PropertyMatcher propertyMatcher, String propertyName, Object expectedValue )
        {
            this.propertyMatcher = propertyMatcher;
            this.propertyName = propertyName;
            this.expectedValue = expectedValue;
        }

        @Override
        protected boolean matchesSafely( Entity entity, Description mismatchDescription )
        {
            if ( !propertyMatcher.matchesSafely( entity, mismatchDescription ) )
            {
                return false;
            }
            if ( transaction != null )
            {
                if ( entity instanceof Node )
                {
                    entity = transaction.getNodeById( entity.getId() );
                }
                else
                {
                    entity = transaction.getRelationshipById( entity.getId() );
                }
            }
            Object foundValue = entity.getProperty( propertyName );
            if ( !propertyValuesEqual( expectedValue, foundValue ) )
            {
                mismatchDescription.appendText( "found value " + formatValue( foundValue ) );
                return false;
            }
            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            propertyMatcher.describeTo( description );
            description.appendText( String.format( "having value %s", formatValue( expectedValue ) ) );
        }

        private boolean propertyValuesEqual( Object expected, Object readValue )
        {
            if ( expected.getClass().isArray() )
            {
                return arrayAsCollection( expected ).equals( arrayAsCollection( readValue ) );
            }
            return expected.equals( readValue );
        }

        private String formatValue( Object v )
        {
            if ( v instanceof String )
            {
                return String.format("'%s'", v.toString());
            }
            return v.toString();
        }

        @Override
        public void setTransaction( Transaction transaction )
        {
            this.transaction = transaction;
            this.propertyMatcher.setTransaction( transaction );
        }
    }

    public static class PropertyMatcher extends TypeSafeDiagnosingMatcher<Entity> implements TransactionalMatcher
    {

        public final String propertyName;
        private Transaction transaction;

        private PropertyMatcher( String propertyName )
        {
            this.propertyName = propertyName;
        }

        @Override
        protected boolean matchesSafely( Entity entity, Description mismatchDescription )
        {
            if ( transaction != null )
            {
                if ( entity instanceof Node )
                {
                    entity = transaction.getNodeById( entity.getId() );
                }
                else
                {
                    entity = transaction.getRelationshipById( entity.getId() );
                }
            }
            if ( !entity.hasProperty( propertyName ) )
            {
                mismatchDescription.appendText( String.format( "found entity with property keys: %s",
                        Iterables.asSet( entity.getPropertyKeys() ) ) );
                return false;
            }
            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( String.format( "entity with property name '%s' ", propertyName ) );
        }

        public PropertyValueMatcher withValue( Object value )
        {
            return new PropertyValueMatcher( this, propertyName, value );
        }

        @Override
        public void setTransaction( Transaction transaction )
        {
            this.transaction = transaction;
        }
    }

    public static PropertyMatcher hasProperty( String propertyName )
    {
        return new PropertyMatcher( propertyName );
    }

    public static Deferred<Node> findNodesByLabelAndProperty( final Label label, final String propertyName, final Object propertyValue,
            final GraphDatabaseService db, Transaction transaction )
    {
        return new Deferred<>()
        {
            @Override
            protected Iterable<Node> manifest()
            {
                return loop( transaction.findNodes( label, propertyName, propertyValue ) );
            }
        };
    }

    public static Deferred<IndexDefinition> getIndexes( final Transaction transaction, final Label label )
    {
        return new Deferred<>()
        {
            @Override
            protected Iterable<IndexDefinition> manifest()
            {
                return transaction.schema().getIndexes( label );
            }
        };
    }

    public static Deferred<String> getPropertyKeys( final Transaction transaction,
                                                    final Entity entity )
    {
        return new Deferred<>()
        {
            @Override
            protected Iterable<String> manifest()
            {
                return entity.getPropertyKeys();
            }
        };
    }

    public static Deferred<ConstraintDefinition> getConstraints( final Transaction transaction, final Label label )
    {
        return new Deferred<>()
        {
            @Override
            protected Iterable<ConstraintDefinition> manifest()
            {
                return transaction.schema().getConstraints( label );
            }
        };
    }

    public static Deferred<ConstraintDefinition> getConstraints( final Transaction transaction,
            final RelationshipType type )
    {
        return new Deferred<>()
        {
            @Override
            protected Iterable<ConstraintDefinition> manifest()
            {
                return transaction.schema().getConstraints( type );
            }
        };
    }

    public static Deferred<ConstraintDefinition> getConstraints( final Transaction transaction )
    {
        return new Deferred<>()
        {
            @Override
            protected Iterable<ConstraintDefinition> manifest()
            {
                return transaction.schema().getConstraints();
            }
        };
    }

    /**
     * Represents test data that can at assertion time produce a collection
     *
     * Useful to defer actually doing operations until context has been prepared (such as a transaction created)
     *
     * @param <T> The type of objects the collection will contain
     */
    public abstract static class Deferred<T>
    {

        protected abstract Iterable<T> manifest();

        public Collection<T> collection()
        {
            return Iterables.asCollection( manifest() );
        }

    }

    @SafeVarargs
    public static <T> TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<T>> containsOnly( final T... expectedObjects )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<T> nodes, Description description )
            {
                Set<T> expected = asSet( expectedObjects );
                Set<T> found = Iterables.asSet( nodes.collection() );
                if ( !expected.equals( found ) )
                {
                    description.appendText( "found " + found.toString() );
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "exactly " + asSet( expectedObjects ) );
            }
        };
    }

    public static TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<?>> hasSize( final int expectedSize )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<?> nodes, Description description )
            {
                int foundSize = nodes.collection().size();

                if ( foundSize != expectedSize )
                {
                    description.appendText( "found " + nodes.collection().toString() );
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "collection of size " + expectedSize );
            }
        };
    }

    public static TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<IndexDefinition>> haveState(
            final Transaction transaction, final Schema.IndexState expectedState )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<IndexDefinition> indexes, Description description )
            {
                for ( IndexDefinition current : indexes.collection() )
                {
                    Schema.IndexState currentState = transaction.schema().getIndexState( current );
                    if ( !currentState.equals( expectedState ) )
                    {
                        description.appendValue( current ).appendText( " has state " ).appendValue( currentState );
                        if ( currentState == Schema.IndexState.FAILED )
                        {
                            String indexFailure = transaction.schema().getIndexFailure( current );
                            description.appendText( " has failure message: " ).appendText( indexFailure );
                        }
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "all indexes have state " + expectedState );
            }
        };
    }

    @SafeVarargs
    public static <T> TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<T>> contains( final T... expectedObjects )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<T> nodes, Description description )
            {
                Set<T> expected = asSet( expectedObjects );
                Set<T> found = Iterables.asSet( nodes.collection() );
                if ( !found.containsAll( expected ) )
                {
                    description.appendText( "found " + found.toString() );
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "contains " + asSet( expectedObjects ) );
            }
        };
    }

    public static TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<?>> isEmpty( )
    {
        return new TypeSafeDiagnosingMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Deferred<?> deferred, Description description )
            {
                Collection<?> collection = deferred.collection();
                if ( !collection.isEmpty() )
                {
                    description.appendText( "was " + collection.toString() );
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "empty collection" );
            }
        };
    }

    public static IndexDefinition createIndex( GraphDatabaseService beansAPI, Label label, String... properties )
    {
        return createIndex( beansAPI, null, label, properties );
    }

    public static IndexDefinition createIndex( GraphDatabaseService beansAPI, String name, Label label, String... properties )
    {
        IndexDefinition indexDef = createIndexNoWait( beansAPI, name, label, properties );
        waitForIndex( beansAPI, indexDef );
        return indexDef;
    }

    public static IndexDefinition createIndexNoWait( GraphDatabaseService beansAPI, Label label, String... properties )
    {
        return createIndexNoWait( beansAPI, null, label, properties );
    }

    public static IndexDefinition createIndexNoWait( GraphDatabaseService db, String name, Label label, String... properties )
    {
        IndexDefinition indexDef;
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator indexCreator = tx.schema().indexFor( label );
            for ( String property : properties )
            {
                indexCreator = indexCreator.on( property );
            }
            if ( name != null )
            {
                indexCreator = indexCreator.withName( name );
            }
            indexDef = indexCreator.create();
            tx.commit();
        }
        return indexDef;
    }

    public static void waitForIndex( GraphDatabaseService beansAPI, IndexDefinition indexDef )
    {
        try ( Transaction tx = beansAPI.beginTx() )
        {
            tx.schema().awaitIndexOnline( indexDef, 30, SECONDS );
        }
    }

    public static void waitForIndexes( GraphDatabaseService beansAPI )
    {
        try ( Transaction tx = beansAPI.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 30, SECONDS );
        }
    }

    public static Object getIndexState( Transaction tx, IndexDefinition indexDef )
    {
        return tx.schema().getIndexState( indexDef );
    }

    public static ConstraintDefinition createConstraint( GraphDatabaseService db, Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.commit();
            return constraint;
        }
    }

    public static Collection<Object> arrayAsCollection( Object arrayValue )
    {
        assert arrayValue.getClass().isArray();

        Collection<Object> result = new ArrayList<>();
        int length = Array.getLength( arrayValue );
        for ( int i = 0; i < length; i++ )
        {
            result.add( Array.get( arrayValue, i ) );
        }
        return result;
    }

    @FunctionalInterface
    private interface TransactionalMatcher
    {
        void setTransaction( Transaction transaction );
    }

    private static class LabelMatcher extends TypeSafeDiagnosingMatcher<Node> implements TransactionalMatcher
    {
        private final Label myLabel;
        private Transaction transaction;

        LabelMatcher( Label myLabel )
        {
            this.myLabel = myLabel;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendValue( myLabel );
        }

        @Override
        protected boolean matchesSafely( Node item, Description mismatchDescription )
        {
            var node = transaction.getNodeById( item.getId() );
            boolean result = node.hasLabel( myLabel );
            if ( !result )
            {
                Set<String> labels = asLabelNameSet( node.getLabels() );
                mismatchDescription.appendText( labels.toString() );
            }
            return result;
        }

        @Override
        public void setTransaction( Transaction transaction )
        {
            this.transaction = transaction;
        }
    }

    private static class LabelsMatcher extends TypeSafeDiagnosingMatcher<Node> implements TransactionalMatcher
    {
        private final Set<String> expectedLabels;
        private Transaction transaction;

        LabelsMatcher( Set<String> expectedLabels )
        {
            this.expectedLabels = expectedLabels;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( expectedLabels.toString() );
        }

        @Override
        protected boolean matchesSafely( Node item, Description mismatchDescription )
        {
            var node = transaction.getNodeById( item.getId() );
            Set<String> foundLabels = asLabelNameSet( node.getLabels() );

            if ( foundLabels.size() == expectedLabels.size() && foundLabels.containsAll( expectedLabels ) )
            {
                return true;
            }

            mismatchDescription.appendText( "was " + foundLabels.toString() );
            return false;
        }

        @Override
        public void setTransaction( Transaction transaction )
        {
            this.transaction = transaction;
        }
    }
}
