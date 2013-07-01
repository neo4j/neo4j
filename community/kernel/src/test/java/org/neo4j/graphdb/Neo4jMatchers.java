/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.neo4j.helpers.Function;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class Neo4jMatchers
{
    public static <T> Matcher<? super T> inTx( final GraphDatabaseService db, final TypeSafeDiagnosingMatcher<T> inner )
    {
        return new DiagnosingMatcher<T>()
        {
            @Override
            protected boolean matches( Object item, Description mismatchDescription )
            {
                Transaction tx = db.beginTx();
                try
                {
                    if ( inner.matches( item ) )
                    {
                        return true;
                    }

                    inner.describeMismatch( item, mismatchDescription );

                    return false;

                }
                finally
                {
                    tx.finish();
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
                if ( !expectedLabels.containsAll( foundLabels ) )
                {
                    mismatchDescription.appendText( "was " + foundLabels.toString() );
                    return false;
                }

                return true;

            }
        };
    }

    public static TypeSafeDiagnosingMatcher<GlobalGraphOperations> hasNoNodes( final Label withLabel )
    {
        return new TypeSafeDiagnosingMatcher<GlobalGraphOperations>()
        {
            @Override
            protected boolean matchesSafely( GlobalGraphOperations glops, Description mismatchDescription )
            {
                Set<Node> found = asSet( glops.getAllNodesWithLabel( withLabel ) );
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

    public static TypeSafeDiagnosingMatcher<GlobalGraphOperations> hasNodes( final Label withLabel, final Node... expectedNodes )
    {
        return new TypeSafeDiagnosingMatcher<GlobalGraphOperations>()
        {
            @Override
            protected boolean matchesSafely( GlobalGraphOperations glops, Description mismatchDescription )
            {
                Set<Node> expected = asSet( expectedNodes );
                Set<Node> found = asSet( glops.getAllNodesWithLabel( withLabel ) );
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

    public static class PropertyValueMatcher extends TypeSafeDiagnosingMatcher<Node>
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
        protected boolean matchesSafely( Node node, Description mismatchDescription )
        {
            if ( !propertyMatcher.matchesSafely( node, mismatchDescription ) )
            {
                return false;
            }

            Object foundValue = node.getProperty( propertyName );
            if ( !foundValue.equals( expectedValue ) )
            {
                mismatchDescription.appendText( "found value " + foundValue );
                return false;
            }
            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            propertyMatcher.describeTo( description );
            description.appendText( String.format( "having value %s", expectedValue.toString() ) );
        }
    }

    public static class PropertyMatcher extends TypeSafeDiagnosingMatcher<Node>
    {

        public final String propertyName;

        private PropertyMatcher( String propertyName )
        {
            this.propertyName = propertyName;
        }

        @Override
        protected boolean matchesSafely( Node node, Description mismatchDescription )
        {
            if ( !node.hasProperty( propertyName ) )
            {
                mismatchDescription.appendText( String.format( "found node without property named '%s'",
                        propertyName ) );
                return false;
            }
            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( String.format( "node with property name '%s' ", propertyName ) );
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
}
