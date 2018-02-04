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

import org.neo4j.test.rule.DatabaseRule;

public abstract class DatabaseExtension<T extends DatabaseRule> extends StatefullFieldExtension<T>
        implements AfterEachCallback, BeforeEachCallback
{
    @Override
    protected abstract Class<T> getFieldType();

    @Override
    protected abstract T createField();

    @Override
    public void afterEach( ExtensionContext context ) throws Exception
    {
        getStoredValue( context ).after( true ); // TODO:always fine for now
        removeStoredValue( context );
    }

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        T databaseRule = getStoredValue( context );
        databaseRule.before();
    }
}
