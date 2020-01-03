/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.test.rule.SuppressOutput;

public class SuppressOutputExtension extends StatefullFieldExtension<SuppressOutput> implements BeforeEachCallback, AfterEachCallback
{
    private static final String SUPPRESS_OUTPUT = "suppressOutput";
    private static final Namespace SUPPRESS_OUTPUT_NAMESPACE = Namespace.create( SUPPRESS_OUTPUT );

    @Override
    protected String getFieldKey()
    {
        return SUPPRESS_OUTPUT;
    }

    @Override
    protected Class<SuppressOutput> getFieldType()
    {
        return SuppressOutput.class;
    }

    @Override
    protected SuppressOutput createField( ExtensionContext extensionContext )
    {
        return SuppressOutput.suppressAll();
    }

    @Override
    protected Namespace getNameSpace()
    {
        return SUPPRESS_OUTPUT_NAMESPACE;
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        getStoredValue( context ).releaseVoices( context.getExecutionException().isPresent() );
        removeStoredValue( context );
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        getStoredValue( context ).captureVoices();
    }
}
