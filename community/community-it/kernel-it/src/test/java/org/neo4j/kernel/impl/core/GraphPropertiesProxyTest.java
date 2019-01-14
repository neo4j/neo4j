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
package org.neo4j.kernel.impl.core;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.GraphTransactionRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.fail;

public class GraphPropertiesProxyTest
{
    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public GraphTransactionRule tx = new GraphTransactionRule( db );

    @Test
    public void testGraphAddPropertyWithNullKey()
    {
        try
        {
            graphProperties().setProperty( null, "bar" );
            fail( "Null key should result in exception." );
        }
        catch ( IllegalArgumentException ignored )
        {
        }
    }

    @Test
    public void testGraphAddPropertyWithNullValue()
    {
        try
        {
            graphProperties().setProperty( "foo", null );
            fail( "Null value should result in exception." );
        }
        catch ( IllegalArgumentException ignored )
        {
        }
        tx.failure();
    }

    private GraphProperties graphProperties()
    {
        return db.getDependencyResolver().resolveDependency( EmbeddedProxySPI.class ).newGraphPropertiesProxy();
    }
}
