//package org.neo4j.server.webadmin.console;
//
//import org.mozilla.javascript.Context;
//import org.mozilla.javascript.ScriptableObject;
//import org.neo4j.server.database.Database;
//
//public class AshSession implements ScriptSession
//{
//    private Context ctx;
//    private ScriptableObject scope;
//    private StringBuilder buffer = new StringBuilder();
//
//    public AshSession( Database database )
//    {
//        ctx = Context.enter();
//        scope = ctx.initStandardObjects();
//        Object wrappedObject = Context.javaToJS( database.graph, scope );
//        ScriptableObject.putProperty( scope, "db", wrappedObject );
//    }
//
//    @Override
//    public String evaluate( String script )
//    {
//        buffer.append( script );
//
//        if ( ctx.stringIsCompilableUnit( buffer.toString() ) )
//        {
//            Object result = ctx.evaluateString( scope, buffer.toString(), "", 1, null );
//            buffer = new StringBuilder(  );
//            return Context.toString( result );
//        }
//        else
//        {
//            return "";
//        }
//    }
//}
