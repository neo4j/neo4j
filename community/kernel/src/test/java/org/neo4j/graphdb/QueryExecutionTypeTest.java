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
package org.neo4j.graphdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.*;
import static org.neo4j.graphdb.QueryExecutionType.*;

@RunWith(Parameterized.class)
public class QueryExecutionTypeTest
{
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> cases()
    {
        return Arrays.asList(
                verify( that( query( READ_ONLY ) )
                                .canContainResults() ),
                verify( that( query( READ_WRITE ) )
                                .canContainResults()
                                .canUpdateData() ),
                verify( that( query( WRITE ) )
                                .canUpdateData() ),
                verify( that( query( SCHEMA_WRITE ) )
                                .canUpdateSchema() ),
                // PROFILE
                verify( that( profiled( READ_ONLY ) )
                                .isExplained()
                                .isProfiled()
                                .canContainResults() ),
                verify( that( profiled( READ_WRITE ) )
                                .isExplained()
                                .isProfiled()
                                .canContainResults()
                                .canUpdateData() ),
                verify( that( profiled( WRITE ) )
                                .isExplained()
                                .isProfiled()
                                .canUpdateData() ),
                verify( that( profiled( SCHEMA_WRITE ) )
                                .isExplained()
                                .isProfiled()
                                .canUpdateSchema() ),
                // EXPLAIN
                verify( that( explained( READ_ONLY ) )
                                .isExplained()
                                .isOnlyExplained() ),
                verify( that( explained( READ_WRITE ) )
                                .isExplained()
                                .isOnlyExplained() ),
                verify( that( explained( WRITE ) )
                                .isExplained()
                                .isOnlyExplained() ),
                verify( that( explained( SCHEMA_WRITE ) )
                                .isExplained()
                                .isOnlyExplained() ),
                // query of EXPLAIN
                verify( thatQueryOf( explained( READ_ONLY ) )
                                .canContainResults() ),
                verify( thatQueryOf( explained( READ_WRITE ) )
                                .canContainResults()
                                .canUpdateData() ),
                verify( thatQueryOf( explained( WRITE ) )
                                .canUpdateData() ),
                verify( thatQueryOf( explained( SCHEMA_WRITE ) )
                                .canUpdateSchema() )
        );
    }

    private final Assumptions expected;

    @Test
    public void verify() throws Exception
    {
        QueryExecutionType executionType = expected.type();
        assertEquals( expected.isProfiled, executionType.isProfiled() );
        assertEquals( expected.requestedExecutionPlanDescription, executionType.requestedExecutionPlanDescription() );
        assertEquals( expected.isExplained, executionType.isExplained() );
        assertEquals( expected.canContainResults, executionType.canContainResults() );
        assertEquals( expected.canUpdateData, executionType.canUpdateData() );
        assertEquals( expected.canUpdateSchema, executionType.canUpdateSchema() );
    }

    @Test
    public void noneOtherLikeIt() throws Exception
    {
        for ( QueryExecutionType.QueryType queryType : QueryExecutionType.QueryType.values() )
        {
            for ( QueryExecutionType type : new QueryExecutionType[]{
                    query( queryType ), profiled( queryType ), explained( queryType )} )
            {
                // the very same object will have the same flags, as will all the explained ones...
                if ( type != expected.type() && !(expected.type().isExplained() && type.isExplained()) )
                {
                    assertFalse(
                            expected.type().toString(),
                            expected.isProfiled == type.isProfiled() &&
                            expected.requestedExecutionPlanDescription == type.requestedExecutionPlanDescription() &&
                            expected.isExplained == type.isExplained() &&
                            expected.canContainResults == type.canContainResults() &&
                            expected.canUpdateData == type.canUpdateData() &&
                            expected.canUpdateSchema == type.canUpdateSchema() );
                }
            }
        }
    }

    public QueryExecutionTypeTest( Assumptions expected )
    {

        this.expected = expected;
    }

    private static Object[] verify( Assumptions assumptions )
    {
        return new Object[]{assumptions};
    }

    private static Assumptions that( QueryExecutionType type )
    {
        return new Assumptions( type, false );
    }

    private static Assumptions thatQueryOf( QueryExecutionType type )
    {
        return new Assumptions( type, true );
    }

    static class Assumptions
    {
        final QueryExecutionType type;
        final boolean convertToQuery;
        boolean isProfiled, requestedExecutionPlanDescription, isExplained, canContainResults, canUpdateData, canUpdateSchema;

        public Assumptions( QueryExecutionType type, boolean convertToQuery )
        {
            this.type = type;
            this.convertToQuery = convertToQuery;
        }

        @Override
        public String toString()
        {
            StringBuilder result = new StringBuilder( type.toString() );
            if ( convertToQuery )
            {
                result.append( " (as query)" );
            }
            String sep = ": ";
            for ( Field field : getClass().getDeclaredFields() )
            {
                if ( field.getType() == boolean.class )
                {
                    boolean value;
                    field.setAccessible( true );
                    try
                    {
                        value = field.getBoolean( this );
                    }
                    catch ( IllegalAccessException e )
                    {
                        throw new RuntimeException( e );
                    }
                    result.append( sep ).append( '.' ).append( field.getName() ).append( "() == " ).append( value );
                    sep = ", ";
                }
            }
            return result.toString();
        }

        public Assumptions isProfiled()
        {
            this.isProfiled = true;
            return this;
        }

        public Assumptions isExplained()
        {
            this.requestedExecutionPlanDescription = true;
            return this;
        }

        public Assumptions isOnlyExplained()
        {
            this.isExplained = true;
            return this;
        }

        public Assumptions canContainResults()
        {
            this.canContainResults = true;
            return this;
        }

        public Assumptions canUpdateData()
        {
            this.canUpdateData = true;
            return this;
        }

        public Assumptions canUpdateSchema()
        {
            this.canUpdateSchema = true;
            return this;
        }

        public QueryExecutionType type()
        {
            return convertToQuery ? query( type.queryType() ) : type;
        }
    }
}