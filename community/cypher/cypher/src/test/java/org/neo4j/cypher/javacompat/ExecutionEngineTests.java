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
package org.neo4j.cypher.javacompat;

import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExecutionEngineTests
{
    @Rule
    public DatabaseRule database = new ImpermanentDatabaseRule();

    @Test
    public void shouldConvertListsAndMapsWhenPassingFromScalaToJava() throws Exception
    {
        ExecutionEngine executionEngine = new ExecutionEngine( database.getGraphDatabaseService() );

        ExecutionResult result = executionEngine.execute( "RETURN { key : 'Value' , " +
                "collectionKey: [{ inner: 'Map1' }, { inner: 'Map2' }]}" );

        Map firstRowValue = (Map) result.iterator().next().values().iterator().next();
        assertThat( (String) firstRowValue.get( "key" ), is( "Value" ) );
        List theList = (List) firstRowValue.get( "collectionKey" );
        assertThat( (String) ((Map) theList.get( 0 )).get( "inner" ), is( "Map1" ) );
        assertThat( (String) ((Map) theList.get( 1 )).get( "inner" ), is( "Map2" ) );
    }
}
