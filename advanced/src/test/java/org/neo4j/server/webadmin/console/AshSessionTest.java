/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

///**
// * Copyright (c) 2002-2010 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with this program. If not, see <http:www.gnu.org/licenses/>.
// */
//
//package org.neo4j.server.webadmin.console;
//
//import org.junit.Before;
//import org.junit.Test;
//import org.neo4j.kernel.ImpermanentGraphDatabase;
//import org.neo4j.server.database.Database;
//
//import static org.hamcrest.CoreMatchers.is;
//import static org.junit.Assert.assertThat;
//
//public class AshSessionTest
//{
//    private Database database;
//    private AshSession session;
//
//    @Test
//    public void shouldBeAbleToSolveSimpleMath() throws Exception
//    {
//        String result = session.evaluate( "5+4" );
//
//        assertThat( result, is( "9" ) );
//    }
//
//    @Test
//    public void shouldHandleMultiLineInput() throws Exception
//    {
//        String result = session.evaluate( "5+" );
//
//        assertThat( result, is( "" ) );
//
//        result = session.evaluate( "4" );
//
//        assertThat( result, is( "9" ) );
//    }
//
//    @Test
//    public void shouldCatchWrappedExceptionsAndPrintThemOut() throws Exception
//    {
//        String result = session.evaluate( "db.getNodeById(666);" );
//        assertThat(result, is("org.neo4j.graphdb.NotFoundException: Node[666]"));
//    }
//
//    @Before
//    public void Init() throws Exception
//    {
//        database = new Database( new ImpermanentGraphDatabase() );
//        session = new AshSession( database );
//    }
//}