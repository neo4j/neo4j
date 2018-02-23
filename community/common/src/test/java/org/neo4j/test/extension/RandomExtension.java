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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.runners.model.MultipleFailureException;

import org.neo4j.helpers.Exceptions;
import org.neo4j.test.rule.RandomRule;

public class RandomExtension extends StatefullFieldExtension<RandomRule>
        implements BeforeEachCallback, AfterEachCallback, TestInstancePostProcessor, TestExecutionExceptionHandler
{
    private static final String RANDOM_STORE_KEY = "random";

    @Override
    public void beforeEach( ExtensionContext context )
    {
        getStoredValue( context ).reset();
    }

    @Override
    protected String getFieldKey()
    {
        return RANDOM_STORE_KEY;
    }

    @Override
    protected Class<RandomRule> getFieldType()
    {
        return RandomRule.class;
    }

    @Override
    protected RandomRule createField()
    {
        return new RandomRule();
    }

    @Override
    public void handleTestExecutionException( ExtensionContext context, Throwable throwable ) throws Throwable
    {
        RandomRule randomRule = getStoredValue( context );
        if ( throwable instanceof MultipleFailureException )
        {
            MultipleFailureException multipleFailures = (MultipleFailureException) throwable;
            for ( Throwable failure : multipleFailures.getFailures() )
            {
                enhanceFailureWithSeed( failure, randomRule );
            }
        }
        else
        {
            enhanceFailureWithSeed( throwable, randomRule );
        }
        throw throwable;
    }

    private void enhanceFailureWithSeed( Throwable t, RandomRule randomRule )
    {
        Exceptions.withMessage( t, t.getMessage() + ": random seed used:" + randomRule.seed() + "L" );
    }

    @Override
    public void afterEach( ExtensionContext context ) throws Exception
    {
        removeStoredValue( context );
    }
}
