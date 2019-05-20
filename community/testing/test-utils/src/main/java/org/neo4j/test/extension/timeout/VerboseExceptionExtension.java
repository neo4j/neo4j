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
package org.neo4j.test.extension.timeout;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.IncompleteExecutionException;

import org.neo4j.test.extension.StatefullFieldExtension;

import static java.lang.String.format;
import static org.neo4j.internal.utils.DumpUtils.threadDump;

public class VerboseExceptionExtension extends StatefullFieldExtension<Void> implements TestExecutionExceptionHandler
{
    private static final String VERBOSE_EXCEPTION = "verboseTimeout";
    private static final ExtensionContext.Namespace VERBOSE_EXCEPTION_NAMESPACE = ExtensionContext.Namespace.create( VERBOSE_EXCEPTION );

    @Override
    protected String getFieldKey()
    {
        return VERBOSE_EXCEPTION;
    }

    @Override
    protected Class<Void> getFieldType()
    {
        return Void.class;
    }

    @Override
    protected Void createField( ExtensionContext extensionContext )
    {
        return null;
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return VERBOSE_EXCEPTION_NAMESPACE;
    }

    @Override
    public void handleTestExecutionException( ExtensionContext context, Throwable throwable ) throws Throwable
    {
        if ( !(throwable instanceof IncompleteExecutionException) )
        {
            System.err.println(
                    format( "=== Test %s-%s timed out, dumping more information ===", context.getRequiredTestMethod().getName(), context.getDisplayName() ) );
            System.err.println( "=== Thread dump ===" );
            System.err.println( threadDump() );
        }
        throw throwable;
    }
}
