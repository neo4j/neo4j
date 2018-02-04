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

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.rule.EmbeddedDatabaseRule;

public class EmbeddedDatabaseExtension extends DatabaseExtension<EmbeddedDatabaseRule> implements BeforeEachCallback
{
    private static final String EMBEDDED_DATABASE_KEY = "embeddedDatabase";

    @Override
    protected String getFieldKey()
    {
        return EMBEDDED_DATABASE_KEY;
    }

    @Override
    protected Class<EmbeddedDatabaseRule> getFieldType()
    {
        return EmbeddedDatabaseRule.class;
    }

    @Override
    protected EmbeddedDatabaseRule createField()
    {
        return new EmbeddedDatabaseRule();
    }

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        EmbeddedDatabaseRule databaseRule = getStoredValue( context );
        databaseRule.prepareDirectory( context.getRequiredTestClass(), context.getRequiredTestMethod().getName() );
        super.beforeEach( context );
    }
}
