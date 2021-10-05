/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.test.extension;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.AssertionFailedError;
import org.opentest4j.TestAbortedException;

import java.lang.annotation.ElementType;
import java.util.Optional;

import org.neo4j.test.RandomSupport;
import org.neo4j.test.RandomSupport.Seed;
import org.neo4j.values.storable.RandomValues;

import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;

public class RandomExtension extends StatefulFieldExtension<RandomSupport>
        implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, TestExecutionExceptionHandler
{
    private static final String RANDOM = "random";
    private static final Namespace RANDOM_NAMESPACE = Namespace.create( RANDOM );

    private final RandomValues.Configuration config;

    public RandomExtension()
    {
        this( new RandomValues.Default() );
    }

    public RandomExtension( RandomValues.Default config )
    {
        this.config = config;
    }

    @Override
    protected String getFieldKey()
    {
        return RANDOM;
    }

    @Override
    protected Class<RandomSupport> getFieldType()
    {
        return RandomSupport.class;
    }

    @Override
    protected Namespace getNameSpace()
    {
        return RANDOM_NAMESPACE;
    }

    @Override
    protected RandomSupport createField( ExtensionContext extensionContext )
    {
        return new RandomSupport().withConfiguration( config );
    }

    @Override
    public void beforeAll( ExtensionContext extensionContext ) throws Exception
    {
        validateAnnotationType( extensionContext, PER_CLASS );
        if ( getLifecycle( extensionContext ) == PER_CLASS )
        {
            setSeed( extensionContext );
        }
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext )
    {
        validateAnnotationType( extensionContext, PER_METHOD );
        if ( getLifecycle( extensionContext ) == PER_METHOD )
        {
            setSeed( extensionContext );
        }
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        removeStoredValue( context );
    }

    @Override
    public void handleTestExecutionException( ExtensionContext context, Throwable t )
    {
        if ( t instanceof TestAbortedException )
        {
            return;
        }

        final long seed = getStoredValue( context ).seed();

        // The reason we throw a new exception wrapping the actual exception here, instead of simply enhancing the message is:
        // - AssertionFailedError has its own 'message' field, in addition to Throwable's 'detailedMessage' field
        // - Even if 'message' field is updated the test doesn't seem to print the updated message on assertion failure
        throw new AssertionFailedError( format( "%s [ random seed used: %dL ]", t.getMessage(), seed ), t );
    }

    private void setSeed( ExtensionContext extensionContext )
    {
        Optional<Seed> optionalSeed = getAnnotatedSeed( extensionContext );
        Long seed = optionalSeed.map( Seed::value ).orElse( System.currentTimeMillis() );
        getStoredValue( extensionContext ).setSeed( seed );
    }

    private static void validateAnnotationType( ExtensionContext extensionContext, TestInstance.Lifecycle lifecycle )
    {
        ElementType expectedType = lifecycle == PER_CLASS ? ElementType.TYPE : ElementType.METHOD;
        if ( getAnnotatedSeed( extensionContext ).isPresent() && getLifecycle( extensionContext ) != lifecycle )
        {
            throw new UnsupportedOperationException( "Annotation @Seed is only allowed on " + expectedType + " when using " + lifecycle );
        }
    }

    private static Optional<Seed> getAnnotatedSeed( ExtensionContext extensionContext )
    {
        return findAnnotation( extensionContext.getElement(), Seed.class );
    }

    private static TestInstance.Lifecycle getLifecycle( ExtensionContext context )
    {
        return context.getTestInstanceLifecycle().orElse( PER_METHOD );
    }
}
