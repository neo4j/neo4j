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

import org.neo4j.test.rule.OtherThreadRule;

public class OtherThreadExtension extends StatefulFieldExtension<OtherThreadRule> implements BeforeEachCallback, AfterEachCallback
{
    private static final String OTHER_THREAD = "otherThread";
    private static final Namespace OTHER_THREAD_NAMESPACE = Namespace.create( "org", "neo4j", OTHER_THREAD );

    @Override
    protected String getFieldKey()
    {
        return OTHER_THREAD;
    }

    @Override
    protected Class<OtherThreadRule> getFieldType()
    {
        return OtherThreadRule.class;
    }

    @Override
    protected OtherThreadRule createField( ExtensionContext extensionContext )
    {
        return new OtherThreadRule();
    }

    @Override
    protected Namespace getNameSpace()
    {
        return OTHER_THREAD_NAMESPACE;
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        var otherThread = getStoredValue( context );
        otherThread.beforeEach( context );
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        var otherThread = getStoredValue( context );
        otherThread.afterEach( context );
    }
}
