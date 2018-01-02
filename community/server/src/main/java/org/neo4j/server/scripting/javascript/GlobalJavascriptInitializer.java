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
package org.neo4j.server.scripting.javascript;

import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.neo4j.server.scripting.UserScriptClassWhiteList;

/**
 * Forgive me Kent Beck, for I have sinned.
 *
 * Rhino's security model means we need to globally define it, so that horridness
 * has been delegated to this class.
 *
 * It allows initializing the global javascript context as either sandboxed or unsafe.
 * If you initialize it twice and ask for different modes, an exception will be thrown
 * as the mode cannot be changed after initialization.
 *
 * We could do some crazy stuff and build a context factory that can be hooked into after
 * the fact and modified, but the plan is to get rid of the ability to switch between unsafe
 * and sandboxed modes, and only allow sandboxed. Unsafe exists for backwards compatibility.
 */
public class GlobalJavascriptInitializer
{

    private static Mode initializationMode;

    public static enum Mode
    {
        SANDBOXED,
        UNSAFE
    }

    public static synchronized void initialize(Mode requestedMode)
    {
        if(initializationMode != null)
        {
            if(initializationMode == requestedMode)
            {
                return;
            }
            else
            {
                throw new RuntimeException( "Cannot initialize javascript context twice, " +
                        "system is currently initialized as: '" + initializationMode.name() + "'." );
            }
        }

        initializationMode = requestedMode;

        ContextFactory contextFactory;
        switch(requestedMode)
        {
            case UNSAFE:
                contextFactory = new ContextFactory()
                {
                    protected Context makeContext()
                    {
                        Context cx = super.makeContext();
                        cx.setLanguageVersion( Context.VERSION_1_7 );
                        // TODO: This goes up to 9, do performance tests to determine appropriate level of optimization
                        cx.setOptimizationLevel( 4 );
                        return cx;
                    }
                };
                break;
            default:
                contextFactory = new ContextFactory()
                {
                    protected Context makeContext()
                    {
                        Context cx = super.makeContext();

                        ClassShutter shutter = new WhiteListClassShutter( UserScriptClassWhiteList.getWhiteList() );

                        cx.setLanguageVersion( Context.VERSION_1_7 );
                        // TODO: This goes up to 9, do performance tests to determine appropriate level of optimization
                        cx.setOptimizationLevel( 4 );
                        cx.setClassShutter( shutter );
                        cx.setWrapFactory( new WhiteListJavaWrapper( shutter ) );
                        return cx;
                    }
                };
                break;
        }

        ContextFactory.initGlobal( contextFactory );
    }

}
