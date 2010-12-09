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
//        assertThat(result, is("9"));
//    }
//
//    @Test
//    public void shouldHandleMultiLineInput() throws Exception
//    {
//        String result = session.evaluate( "5+" );
//
//        assertThat(result, is(""));
//
//        result = session.evaluate( "4" );
//
//        assertThat( result, is("9") );
//    }
//
//    @Before
//    public void Init() throws Exception
//    {
//        database = new Database( new ImpermanentGraphDatabase() );
//        session = new AshSession( database );
//    }
//}
