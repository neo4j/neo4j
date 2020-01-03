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
package org.neo4j.test.rule.concurrent;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.extension.StatefullFieldExtension;

public class ThreadingExtension  extends StatefullFieldExtension<ThreadingRule>
        implements BeforeEachCallback, AfterEachCallback, AfterAllCallback
{
    private static final String THREADING = "threading";
    private static final ExtensionContext.Namespace THREADING_NAMESPACE = ExtensionContext.Namespace.create( THREADING );
    @Override
    public void afterEach( ExtensionContext extensionContext ) throws Exception
    {
        ThreadingRule threadingRule = getStoredValue( extensionContext );
        threadingRule.after();
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext ) throws Exception
    {
        ThreadingRule threadingRule = getStoredValue( extensionContext );
        threadingRule.before();
    }

    @Override
    protected String getFieldKey()
    {
        return THREADING;
    }

    @Override
    protected Class<ThreadingRule> getFieldType()
    {
        return ThreadingRule.class;
    }

    @Override
    protected ThreadingRule createField( ExtensionContext extensionContext )
    {
        ThreadingRule threadingRule = new ThreadingRule();
        return threadingRule;
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace()
    {
        return THREADING_NAMESPACE;
    }
}
