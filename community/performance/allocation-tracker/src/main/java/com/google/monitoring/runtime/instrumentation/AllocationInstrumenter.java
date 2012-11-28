/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package com.google.monitoring.runtime.instrumentation;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;

/**
 * Instruments bytecodes that allocate heap memory to call a recording hook.
 * This will add a static invocation to a recorder function to any bytecode that
 * looks like it will be allocating heap memory allowing users to implement heap
 * profiling schemes.
 *
 * @author Ami Fischman
 * @author Jeremy Manson
 */
public class AllocationInstrumenter implements ClassFileTransformer
{
    static final Logger logger =
            Logger.getLogger( AllocationInstrumenter.class.getName() );

    // We can rewrite classes loaded by the bootstrap class loader
    // iff the agent is loaded by the bootstrap class loader.  It is
    // always *supposed* to be loaded by the bootstrap class loader, but
    // this relies on the Boot-Class-Path attribute in the JAR file always being
    // set to the name of the JAR file that contains this agent, which we cannot
    // guarantee programmatically.
    private static volatile boolean canRewriteBootstrap;

    private static Collection<String> blackList = new LinkedList<String>();

    static
    {
        blackList.add( "java/lang/ThreadLocal" );
        blackList.add( "sun/nio/ch/" );
        blackList.add( "ognl/" );
    }

    static boolean canRewriteClass( String className, ClassLoader loader )
    {
        // There are two conditions under which we don't rewrite:
        //  1. If className was loaded by the bootstrap class loader and
        //  the agent wasn't (in which case the class being rewritten
        //  won't be able to call agent methods).
        //  2. If the class is blacklisted, for instance certain
        //  core Java classes where the JVM borks if we change them.
        if ( ((loader == null) && !canRewriteBootstrap) ||
             isBlackListed( className ) )
        {
            System.err.println( "Skipping: " + className );
            return false;
        }

        System.err.println( "Rewriting: " + className );
        return true;
    }

    private static boolean isBlackListed( String className )
    {
        for ( String blackListItem : blackList )
        {
            if(className.startsWith( blackListItem ))
            {
                return true;
            }
        }

        return false;
    }

    // No instantiating me except in premain() or in {@link JarClassTransformer}.
    AllocationInstrumenter()
    {
    }

    public static void premain( String agentArgs, Instrumentation inst )
    {
        AllocationRecorder.setInstrumentation( inst );

        // Force eager class loading here; we need this class to do
        // instrumentation, so if we don't do the eager class loading, we
        // get a ClassCircularityError when trying to load and instrument
        // this class.
        try
        {
            Class.forName( "sun.security.provider.PolicyFile" );
        }
        catch ( Throwable t )
        {
            // NOP
        }

        if ( !inst.isRetransformClassesSupported() )
        {
            System.err.println( "Some JDK classes are already loaded and " +
                    "will not be instrumented." );
        }

        // Don't try to rewrite classes loaded by the bootstrap class
        // loader if this class wasn't loaded by the bootstrap class
        // loader.
        if ( AllocationRecorder.class.getClassLoader() != null )
        {
            canRewriteBootstrap = false;
            // The loggers aren't installed yet, so we use println.
            System.err.println( "Class loading breakage: " +
                    "Will not be able to instrument JDK classes" );
            return;
        }

        canRewriteBootstrap = true;

        inst.addTransformer( new ConstructorInstrumenter(),
                inst.isRetransformClassesSupported() );

        List<String> args = Arrays.asList(
                agentArgs == null ? new String[0] : agentArgs.split( "," ) );
        if ( !args.contains( "manualOnly" ) )
        {
            bootstrap( inst );
        }
    }

    private static void bootstrap( Instrumentation inst )
    {
        inst.addTransformer( new AllocationInstrumenter(),
                inst.isRetransformClassesSupported() );

        if ( !canRewriteBootstrap )
        {
            return;
        }

        // Get the set of already loaded classes that can be rewritten.
        Class<?>[] classes = inst.getAllLoadedClasses();
        ArrayList<Class<?>> classList = new ArrayList<Class<?>>();
        for ( int i = 0; i < classes.length; i++ )
        {
            if ( inst.isModifiableClass( classes[i] ) )
            {
                classList.add( classes[i] );
            }
        }

        // Reload classes, if possible.
        Class<?>[] workaround = new Class<?>[classList.size()];
        try
        {
            inst.retransformClasses( classList.toArray( workaround ) );
        }
        catch ( UnmodifiableClassException e )
        {
            System.err.println( "AllocationInstrumenter was unable to " +
                    "retransform early loaded classes." );
        }


    }

    @Override
    public byte[] transform(
            ClassLoader loader, String className, Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain, byte[] origBytes )
    {
        if ( !canRewriteClass( className, loader ) )
        {
            return null;
        }

        return instrument( origBytes, loader );
    }

    /**
     * Given the bytes representing a class, go through all the bytecode in it and
     * instrument any occurences of new/newarray/anewarray/multianewarray with
     * pre- and post-allocation hooks.  Even more fun, intercept calls to the
     * reflection API's Array.newInstance() and instrument those too.
     *
     * @param originalBytes  the original <code>byte[]</code> code.
     * @param recorderClass  the <code>String</code> internal name of the class
     *                       containing the recorder method to run.
     * @param recorderMethod the <code>String</code> name of the recorder method
     *                       to run.
     * @return the instrumented <code>byte[]</code> code.
     */
    public static byte[] instrument( byte[] originalBytes, String recorderClass,
                                     String recorderMethod, ClassLoader loader )
    {
        try
        {
            ClassReader cr = new ClassReader( originalBytes );
            // The verifier in JDK7 requires accurate stackmaps, so we use
            // COMPUTE_FRAMES.
            ClassWriter cw =
                    new StaticClassWriter( cr, ClassWriter.COMPUTE_FRAMES, loader );

            VerifyingClassAdapter vcw =
                    new VerifyingClassAdapter( cw, originalBytes, cr.getClassName() );
            ClassVisitor adapter =
                    new AllocationClassAdapter( vcw, recorderClass, recorderMethod );

            cr.accept( adapter, ClassReader.SKIP_FRAMES );

            return vcw.toByteArray();
        }
        catch ( RuntimeException e )
        {
            logger.log( Level.WARNING, "Failed to instrument class.", e );
            throw e;
        }
        catch ( Error e )
        {
            logger.log( Level.WARNING, "Failed to instrument class.", e );
            throw e;
        }
    }


    /**
     * @see #instrument(byte[], String, String, ClassLoader)
     *      documentation for the 4-arg version.  This is a convenience
     *      version that uses the recorder in this class.
     */
    public static byte[] instrument( byte[] originalBytes, ClassLoader loader )
    {
        return instrument(
                originalBytes,
                "com/google/monitoring/runtime/instrumentation/AllocationRecorder",
                "recordAllocation",
                loader );
    }
}
