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

import org.neo4j.test.rule.SuppressOutput;

public class SuppressOutputExtension extends StatefullFieldExtension<SuppressOutput> implements BeforeEachCallback,
        AfterEachCallback, TestExecutionExceptionHandler
{
    private static final String SUPPRESS_OUTPUT_STORE_KEY = "suppressOutput";
    private static final String SUPPRESS_OUTPUT_EXCEPTION_KEY = "suppressOutputException";

    @Override
    protected String getFieldKey()
    {
        return SUPPRESS_OUTPUT_STORE_KEY;
    }

    @Override
    protected Class<SuppressOutput> getFieldType()
    {
        return SuppressOutput.class;
    }

    @Override
    protected SuppressOutput createField()
    {
        return SuppressOutput.suppressAll();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        Boolean exceptionThrown = getStore( context )
                .getOrComputeIfAbsent( SUPPRESS_OUTPUT_EXCEPTION_KEY, s -> Boolean.FALSE, Boolean.class );
        getStoredValue( context ).releaseVoices( exceptionThrown );
        removeStoredValue( context );
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        getStoredValue( context ).captureVoices();
    }

    @Override
    public void handleTestExecutionException( ExtensionContext context, Throwable throwable ) throws Throwable
    {
        getStore( context ).put( SUPPRESS_OUTPUT_EXCEPTION_KEY, Boolean.TRUE );
        throw throwable;
    }
}
