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
