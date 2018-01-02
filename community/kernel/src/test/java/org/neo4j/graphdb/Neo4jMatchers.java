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

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Function;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class Neo4jMatchers
{
    public static <T> Matcher<? super T> inTx( final GraphDatabaseService db, final Matcher<T> inner )
    {
        return inTx( db, inner, false );
    }

    public static <T> Matcher<? super T> inTx( final GraphDatabaseService db, final Matcher<T> inner,
            final boolean successful )
    {
        return new DiagnosingMatcher<T>()
        {
            @Override
            protected boolean matches( Object item, Description mismatchDescription )
            {
                try ( Transaction ignored = db.beginTx() )
                {
                    if ( inner.matches( item ) )
                    {
                        if ( successful )
                        {
                            ignored.success();
                        }
                        return true;
                    }

                    inner.describeMismatch( item, mismatchDescription );

                    if ( successful )
                    {
                        ignored.success();
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
        return new TypeSafeDiagnosingMatcher<Node>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendValue( myLabel );
            }

            @Override
            protected boolean matchesSafely( Node item, Description mismatchDescription )
            {
                boolean result = item.hasLabel( myLabel );
                if ( !result )
                {
                    Set<String> labels = asLabelNameSet( item.getLabels() );
                    mismatchDescription.appendText( labels.toString() );
                }
                return result;
            }
        };
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
        return hasLabels( emptySetOf( String.class ) );
    }

    public static TypeSafeDiagnosingMatcher<Node> hasLabels( final Set<String> expectedLabels )
    {
        return new TypeSafeDiagnosingMatcher<Node>()
        {
            private Set<String> foundLabels;

            @Override
            public void describeTo( Description description )
            {
                description.appendText( expectedLabels.toString() );
            }

            @Override
            protected boolean matchesSafely( Node item, Description mismatchDescription )
            {
                foundLabels = asLabelNameSet( item.getLabels() );

                if ( foundLabels.size() == expectedLabels.size() && foundLabels.containsAll( expectedLabels ) )
                {
                    return true;
                }

                mismatchDescription.appendText( "was " + foundLabels.toString() );
                return false;
            }
        };
    }

    public static TypeSafeDiagnosingMatcher<GraphDatabaseService> hasNoNodes( final Label withLabel )
    {
        return new TypeSafeDiagnosingMatcher<GraphDatabaseService>()
        {
            @Override
            protected boolean matchesSafely( GraphDatabaseService db, Description mismatchDescription )
            {
                Set<Node> found = asSet( db.findNodes( withLabel ) );
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

    public static TypeSafeDiagnosingMatcher<GraphDatabaseService> hasNodes( final Label withLabel, final Node... expectedNodes )
    {
        return new TypeSafeDiagnosingMatcher<GraphDatabaseService>()
        {
            @Override
            protected boolean matchesSafely( GraphDatabaseService db, Description mismatchDescription )
            {
                Set<Node> expected = asSet( expectedNodes );
                Set<Node> found = asSet( db.findNodes( withLabel ) );
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
        return asSet( map( new Function<Label, String>()
        {
            @Override
            public String apply( Label from )
            {
                return from.name();
            }
        }, enums ) );
    }

    public static Matcher<? super Iterator<Long>> hasSamePrimitiveItems( final PrimitiveLongIterator actual )
    {
        return new TypeSafeDiagnosingMatcher<Iterator<Long>>()
        {
            int len = 0;

            String actualText = null;
            String expectedText = null;

            @Override
            protected boolean matchesSafely( Iterator<Long> expected, Description actualDescription )
            {

                if ( actualText != null )
                {
                    actualDescription.appendText( actualText );

                }
                // compare iterators element-wise
                while ( expected.hasNext() && actual.hasNext() )
                {
                    len++;

                    Long expectedNext = expected.next();
                    long actualNext = actual.next();

                    if ( !(expectedNext.equals( actualNext )) )
                    {
                         actualText = format( "Element %d at position %d", actualNext, len );
                         expectedText = format( "Element %d at position %d", expectedNext, len );

                         return false;
                    }

                }

                // check that the iterators do not have a different length
                if ( expected.hasNext() )
                {
                    actualText = format("Length %d", len );
                    expectedText = format( "Length %d", len + 1 );

                    return false;
                }

                if ( actual.hasNext() )
                {
                    actualText = format("Length %d", len + 1 );
                    expectedText = format( "Length %d", len );

                    return false;
                }

                return true;
            }

            @Override
            public void describeTo( Description expectedDescription )
            {
                if ( expectedText != null )
                {
                    expectedDescription.appendText( expectedText );
                }
            }
        };
    }

    public static class PropertyValueMatcher extends TypeSafeDiagnosingMatcher<PropertyContainer>
    {
        private final PropertyMatcher propertyMatcher;
        private final String propertyName;
        private final Object expectedValue;

        private PropertyValueMatcher( PropertyMatcher propertyMatcher, String propertyName, Object expectedValue )
        {
            this.propertyMatcher = propertyMatcher;
            this.propertyName = propertyName;
            this.expectedValue = expectedValue;
        }

        @Override
        protected boolean matchesSafely( PropertyContainer propertyContainer, Description mismatchDescription )
        {
            if ( !propertyMatcher.matchesSafely( propertyContainer, mismatchDescription ) )
            {
                return false;
            }

            Object foundValue = propertyContainer.getProperty( propertyName );
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

        private String formatValue(Object v)
        {
            if (v instanceof String)
            {
                return String.format("'%s'", v.toString());
            }
            return v.toString();
        }

    }

    public static class PropertyMatcher extends TypeSafeDiagnosingMatcher<PropertyContainer>
    {

        public final String propertyName;

        private PropertyMatcher( String propertyName )
        {
            this.propertyName = propertyName;
        }

        @Override
        protected boolean matchesSafely( PropertyContainer propertyContainer, Description mismatchDescription )
        {
            if ( !propertyContainer.hasProperty( propertyName ) )
            {
                mismatchDescription.appendText( String.format( "found property container with property keys: %s",
                        asSet( propertyContainer.getPropertyKeys() ) ) );
                return false;
            }
            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( String.format( "property container with property name '%s' ", propertyName ) );
        }

        public PropertyValueMatcher withValue( Object value )
        {
            return new PropertyValueMatcher( this, propertyName, value );
        }
    }

    public static PropertyMatcher hasProperty( String propertyName )
    {
        return new PropertyMatcher( propertyName );
    }

    public static Deferred<Node> findNodesByLabelAndProperty( final Label label, final String propertyName,
                                                              final Object propertyValue,
                                                              final GraphDatabaseService db )
    {
        return new Deferred<Node>(db)
        {
            @Override
            protected Iterable<Node> manifest()
            {
                return loop( db.findNodes( label, propertyName, propertyValue ) );
            }
        };
    }

    public static Deferred<IndexDefinition> getIndexes( final GraphDatabaseService db, final Label label )
    {
        return new Deferred<IndexDefinition>( db )
        {
            @Override
            protected Iterable<IndexDefinition> manifest()
            {
                return db.schema().getIndexes( label );
            }
        };
    }

    public static Deferred<String> getPropertyKeys( final GraphDatabaseService db,
                                                    final PropertyContainer propertyContainer )
    {
        return new Deferred<String>( db )
        {
            @Override
            protected Iterable<String> manifest()
            {
                return propertyContainer.getPropertyKeys();
            }
        };
    }

    public static Deferred<ConstraintDefinition> getConstraints( final GraphDatabaseService db, final Label label )
    {
        return new Deferred<ConstraintDefinition>( db )
        {
            @Override
            protected Iterable<ConstraintDefinition> manifest()
            {
                return db.schema().getConstraints( label );
            }
        };
    }

    public static Deferred<ConstraintDefinition> getConstraints( final GraphDatabaseService db,
            final RelationshipType type )
    {
        return new Deferred<ConstraintDefinition>( db )
        {
            @Override
            protected Iterable<ConstraintDefinition> manifest()
            {
                return db.schema().getConstraints( type );
            }
        };
    }

    public static Deferred<ConstraintDefinition> getConstraints( final GraphDatabaseService db )
    {
        return new Deferred<ConstraintDefinition>( db )
        {
            @Override
            protected Iterable<ConstraintDefinition> manifest()
            {
                return db.schema().getConstraints( );
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
    public static abstract class Deferred<T>
    {

        private final GraphDatabaseService db;

        public Deferred( GraphDatabaseService db )
        {
            this.db = db;
        }

        protected abstract Iterable<T> manifest();

        public Collection<T> collection()
        {
            try ( Transaction ignore = db.beginTx() )
            {
                return asCollection( manifest() );
            }
        }

    }

    @SafeVarargs
    public static <T> TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<T>> containsOnly( final T... expectedObjects )
    {
        return new TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<T>>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<T> nodes, Description description )
            {
                Set<T> expected = asSet( expectedObjects );
                Set<T> found = asSet( nodes.collection() );
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
        return new TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<?>>()
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
            final GraphDatabaseService db, final Schema.IndexState expectedState )
    {
        return new TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<IndexDefinition>>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<IndexDefinition> indexes, Description description )
            {
                for ( IndexDefinition current : indexes.collection() )
                {
                    Schema.IndexState currentState = db.schema().getIndexState( current );
                    if ( !currentState.equals( expectedState ) )
                    {
                        description.appendValue( current ).appendText( " has state " ).appendValue( currentState );
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
        return new TypeSafeDiagnosingMatcher<Neo4jMatchers.Deferred<T>>()
        {
            @Override
            protected boolean matchesSafely( Neo4jMatchers.Deferred<T> nodes, Description description )
            {
                Set<T> expected = asSet( expectedObjects );
                Set<T> found = asSet( nodes.collection() );
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
        return new TypeSafeDiagnosingMatcher<Deferred<?>>()
        {
            @Override
            protected boolean matchesSafely( Deferred<?> deferred, Description description )
            {
                Collection<?> collection = deferred.collection();
                if(!collection.isEmpty())
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

    public static IndexDefinition createIndex( GraphDatabaseService beansAPI, Label label, String property )
    {
        IndexDefinition indexDef;
        try ( Transaction tx = beansAPI.beginTx() )
        {
            indexDef = beansAPI.schema().indexFor( label ).on( property ).create();
            tx.success();
        }

        waitForIndex( beansAPI, indexDef );
        return indexDef;
    }

    public static void waitForIndex( GraphDatabaseService beansAPI, IndexDefinition indexDef )
    {
        try ( Transaction ignored = beansAPI.beginTx() )
        {
            beansAPI.schema().awaitIndexOnline( indexDef, 10, SECONDS );
        }
    }

    public static Object getIndexState( GraphDatabaseService beansAPI, IndexDefinition indexDef )
    {
        try ( Transaction ignored = beansAPI.beginTx() )
        {
            return beansAPI.schema().getIndexState( indexDef );
        }
    }

    public static ConstraintDefinition createConstraint( GraphDatabaseService db, Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint =
                    db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
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
}
