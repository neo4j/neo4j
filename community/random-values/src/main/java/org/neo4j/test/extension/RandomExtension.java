/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.AssertionFailedError;

import org.neo4j.test.rule.RandomRule;

import static java.lang.String.format;

public class RandomExtension extends StatefullFieldExtension<RandomRule> implements AfterEachCallback, TestExecutionExceptionHandler
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
    public void afterEach( ExtensionContext context )
    {
        removeStoredValue( context );
    }

    @Override
    public void handleTestExecutionException( ExtensionContext context, Throwable t )
    {
        final long seed = getStoredValue( context ).seed();
        throw new AssertionFailedError( format( "%s [ random seed used: %dL ]", t.getMessage(), seed ), t );
    }
}
