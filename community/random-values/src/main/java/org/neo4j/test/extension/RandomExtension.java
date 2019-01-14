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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.AssertionFailedError;
import org.opentest4j.TestAbortedException;

import java.util.Optional;

import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RandomRule.Seed;

import static java.lang.String.format;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;

public class RandomExtension extends StatefullFieldExtension<RandomRule> implements BeforeEachCallback, AfterEachCallback, TestExecutionExceptionHandler
{
    private static final String RANDOM = "random";
    private static final Namespace RANDOM_NAMESPACE = Namespace.create( RANDOM );

    @Override
    protected String getFieldKey()
    {
        return RANDOM;
    }

    @Override
    protected Class<RandomRule> getFieldType()
    {
        return RandomRule.class;
    }

    @Override
    protected Namespace getNameSpace()
    {
        return RANDOM_NAMESPACE;
    }

    @Override
    protected RandomRule createField( ExtensionContext extensionContext )
    {
        RandomRule randomRule = new RandomRule();
        randomRule.setSeed( System.currentTimeMillis() );
        randomRule.reset();
        return randomRule;
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext )
    {
        Optional<Seed> optionalSeed = findAnnotation( extensionContext.getElement(), Seed.class );
        optionalSeed.map( Seed::value ).ifPresent( seed -> getStoredValue( extensionContext ).setSeed( seed ) );
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
}
