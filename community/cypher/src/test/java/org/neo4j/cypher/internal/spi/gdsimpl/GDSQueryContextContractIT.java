/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
///**
// * Copyright (c) 2002-2013 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.cypher.internal.spi.gdsimpl;
//
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.neo4j.cypher.internal.spi.QueryContext;
//import org.neo4j.cypher.internal.spi.QueryContextContract;
//import org.neo4j.graphdb.Transaction;
//import org.neo4j.test.ImpermanentGraphDatabase;
//
//public class GDSQueryContextContractIT extends QueryContextContract
//{
//
//    private static ImpermanentGraphDatabase gdb;
//    private QueryContext ctx;
//    private Transaction tx;
//
//    @BeforeClass
//    public static void createDb()
//    {
//        gdb = new ImpermanentGraphDatabase( );
//    }
//
//
//    @Before
//    public void createQueryContext()
//    {
//        tx = gdb.beginTx();
//        ctx = new GDSBackedQueryContext( gdb );
//    }
//
//    @After
//    public void clean()
//    {
//        ctx.close();
//        tx.finish();
//    }
//
//    @AfterClass
//    public static void destroy()
//    {
//        gdb.shutdown();
//    }
//
//
//    @Override
//    public QueryContext ctx()
//    {
//        return ctx;
//    }
//
//
//}
