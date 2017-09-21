/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tooling.procedure;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.tooling.procedure.procedures.valid.Procedures;

import static org.assertj.core.api.Assertions.assertThat;


public class ProcedureTest
{

    private static final Class<?> PROCEDURES_CLASS = Procedures.class;

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public Neo4jRule graphDb = new Neo4jRule().withProcedure( PROCEDURES_CLASS );
    private String procedureNamespace = PROCEDURES_CLASS.getPackage().getName();

    @Test
    public void calls_simplistic_procedure()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {

            StatementResult result = session.run( "CALL " + procedureNamespace + ".theAnswer()" );

            assertThat( result.single().get( "value" ).asLong() ).isEqualTo( 42L );
        }
    }

    @Test
    public void calls_procedures_with_simple_input_type_returning_void()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {

            session.run( "CALL " + procedureNamespace + ".simpleInput00()" );
            session.run( "CALL " + procedureNamespace + ".simpleInput01('string')" );
            session.run( "CALL " + procedureNamespace + ".simpleInput02(42)" );
            session.run( "CALL " + procedureNamespace + ".simpleInput03(42)" );
            session.run( "CALL " + procedureNamespace + ".simpleInput04(4.2)" );
            session.run( "CALL " + procedureNamespace + ".simpleInput05(true)" );
            session.run( "CALL " + procedureNamespace + ".simpleInput06(false)" );
            session.run( "CALL " + procedureNamespace + ".simpleInput07({foo:'bar'})" );
            session.run( "MATCH (n)            CALL " + procedureNamespace + ".simpleInput08(n) RETURN n" );
            session.run( "MATCH p=(()-[r]->()) CALL " + procedureNamespace + ".simpleInput09(p) RETURN p" );
            session.run( "MATCH ()-[r]->()     CALL " + procedureNamespace + ".simpleInput10(r) RETURN r" );
        }
    }

    @Test
    public void calls_procedures_with_different_modes_returning_void()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {
            session.run( "CALL " + procedureNamespace + ".performsWrites()" );
            session.run( "CALL " + procedureNamespace + ".defaultMode()" );
            session.run( "CALL " + procedureNamespace + ".readMode()" );
            session.run( "CALL " + procedureNamespace + ".writeMode()" );
            session.run( "CALL " + procedureNamespace + ".schemaMode()" );
            session.run( "CALL " + procedureNamespace + ".dbmsMode()" );
        }
    }

    @Test
    public void calls_procedures_with_simple_input_type_returning_record_with_primitive_fields()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {

            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput11('string') YIELD field04 AS p RETURN p" ).single() ).isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput12(42)" ).single() ).isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput13(42)" ).single() ).isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput14(4.2)" ).single() ).isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput15(true)" ).single() ).isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput16(false)" ).single() ).isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput17({foo:'bar'})" ).single() )
                    .isNotNull();
            assertThat( session.run( "CALL " + procedureNamespace + ".simpleInput21()" ).single() ).isNotNull();
        }

    }

    private Config configuration()
    {
        return Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig();
    }

}
