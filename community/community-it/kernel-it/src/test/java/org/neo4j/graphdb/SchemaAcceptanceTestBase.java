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
package org.neo4j.graphdb;

import java.util.function.Consumer;

import org.neo4j.graphdb.schema.Schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaAcceptanceTestBase
{
    protected final String propertyKey = "my_property_key";
    protected final String secondPropertyKey = "my_second_property_key";
    protected Label label = Labels.MY_LABEL;
    protected Label otherLabel = Labels.MY_OTHER_LABEL;
    protected RelationshipType relType = RelationshipType.withName( "relType" );

    protected <EXCEPTION extends Throwable, CAUSE extends Throwable> void assertExpectedException( Class<CAUSE> expectedCause, String expectedMessage,
            EXCEPTION exception )
    {
        final Throwable cause = exception.getCause();
        assertEquals( expectedCause, cause.getClass(),
                "Expected cause to be of type " + expectedCause + " but was " + cause.getClass() );
        assertEquals( expectedMessage, exception.getMessage() );
    }

    protected enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    protected enum SchemaTxStrategy
    {
        SEPARATE_TX
                {
                    public <EXCEPTION extends Throwable> EXCEPTION execute(
                            GraphDatabaseService db, Consumer<Schema> firstSchemaRule, Consumer<Schema> secondSchemaRule, Class<EXCEPTION> expectedException )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            firstSchemaRule.accept( tx.schema() );
                            tx.commit();
                        }
                        return assertThrows( expectedException, () -> {
                            try ( Transaction tx = db.beginTx() )
                            {
                                secondSchemaRule.accept( tx.schema() );
                                tx.commit();
                            }
                        } );
                    }
                },
        SAME_TX
                {
                    public <EXCEPTION extends Throwable> EXCEPTION execute(
                            GraphDatabaseService db, Consumer<Schema> firstSchemaRule, Consumer<Schema> secondSchemaRule, Class<EXCEPTION> expectedException )
                    {
                        return assertThrows( expectedException, () -> {
                            try ( Transaction tx = db.beginTx() )
                            {
                                firstSchemaRule.accept( tx.schema() );
                                secondSchemaRule.accept( tx.schema() );
                                tx.commit();
                            }
                        } );
                    }
                };

        public abstract <EXCEPTION extends Throwable> EXCEPTION execute(
                GraphDatabaseService db, Consumer<Schema> firstSchemaRule, Consumer<Schema> secondSchemaRule, Class<EXCEPTION> expectedException );
    }
}
