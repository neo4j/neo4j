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
// Portions Copyright 2011 Google, Inc.
//
// This is an extracted version of the ClassInfo and ClassWriter
// portions of ClassWriterComputeFramesTest in the set of ASM tests.
// We have done a fair bit of rewriting for readability, and changed
// the comments.  The original author is Eric Bruneton.


package com.google.monitoring.runtime.instrumentation;

import java.io.IOException;
import java.io.InputStream;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * A {@link ClassWriter} that looks for static class data in the
 * classpath when the classes are not available at runtime.
 * <p/>
 * <p>ClassWriter uses class hierarchy information, which it gets by
 * looking at loaded classes, to make some decisions about the best
 * way to write classes.  The problem with this is that it fails if
 * the superclass hasn't been loaded yet.  StaticClassWriter fails
 * over to looking for the class hierarchy information in the
 * ClassLoader's resources (usually the classpath) if the class it
 * needs hasn't been loaded yet.
 * <p/>
 * <p>This class was heavily influenced by ASM's
 * org.objectweb.asm.util.ClassWriterComputeFramesTest, which contains
 * the same logic in a subclass.  The code here has been slightly
 * cleaned up for readability.
 *
 * @author jeremymanson@google.com (Jeremy Manson)
 */
class StaticClassWriter extends ClassWriter
{

    /* The classloader that we use to look for the unloaded class */
    private final ClassLoader classLoader;

    /**
     * {@inheritDoc}
     *
     * @param classLoader the class loader that loaded this class
     */
    public StaticClassWriter(
            ClassReader classReader, int flags, ClassLoader classLoader )
    {
        super( classReader, flags );
        this.classLoader = classLoader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getCommonSuperClass(
            final String type1, final String type2 )
    {
        try
        {
            return super.getCommonSuperClass( type1, type2 );
        }
        catch ( Throwable e )
        {
            // Try something else...
        }
        // Exactly the same as in ClassWriter, but gets the superclass
        // directly from the class file.
        ClassInfo ci1, ci2;
        try
        {
            ci1 = new ClassInfo( type1, classLoader );
            ci2 = new ClassInfo( type2, classLoader );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
        if ( ci1.isAssignableFrom( ci2 ) )
        {
            return type1;
        }
        if ( ci2.isAssignableFrom( ci1 ) )
        {
            return type2;
        }
        if ( ci1.isInterface() || ci2.isInterface() )
        {
            return "java/lang/Object";
        }

        do
        {
            // Should never be null, because if ci1 were the Object class
            // or an interface, it would have been caught above.
            ci1 = ci1.getSuperclass();
        } while ( !ci1.isAssignableFrom( ci2 ) );
        return ci1.getType().getInternalName();
    }

    /**
     * For a given class, this stores the information needed by the
     * getCommonSuperClass test.  This determines if the class is
     * available at runtime, and then, if it isn't, it tries to get the
     * class file, and extract the appropriate information from that.
     */
    static class ClassInfo
    {

        private final Type type;
        private final ClassLoader loader;
        private final boolean isInterface;
        private final String superClass;
        private final String[] interfaces;

        public ClassInfo( String type, ClassLoader loader )
        {
            Class cls = null;
            // First, see if we can extract the information from the class...
            try
            {
                cls = Class.forName( type );
            }
            catch ( Exception e )
            {
                // failover...
            }

            if ( cls != null )
            {
                this.type = Type.getType( cls );
                this.loader = loader;
                this.isInterface = cls.isInterface();
                this.superClass = cls.getSuperclass().getName();
                Class[] ifs = cls.getInterfaces();
                this.interfaces = new String[ifs.length];
                for ( int i = 0; i < ifs.length; i++ )
                {
                    this.interfaces[i] = ifs[i].getName();
                }
                return;
            }

            // The class isn't loaded.  Try to get the class file, and
            // extract the information from that.
            this.loader = loader;
            this.type = Type.getObjectType( type );
            String fileName = type.replace( '.', '/' ) + ".class";
            InputStream is = null;
            ClassReader cr;
            try
            {
                is = (loader == null) ?
                        ClassLoader.getSystemResourceAsStream( fileName ) :
                        loader.getResourceAsStream( fileName );
                cr = new ClassReader( is );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( is != null )
                {
                    try
                    {
                        is.close();
                    }
                    catch ( Exception e )
                    {
                    }
                }
            }

            int offset = cr.header;
            isInterface = (cr.readUnsignedShort( offset ) & Opcodes.ACC_INTERFACE) != 0;
            char[] buf = new char[2048];

            // Read the superclass
            offset += 4;
            superClass = readConstantPoolString( cr, offset, buf );

            // Read the interfaces
            offset += 2;
            int numInterfaces = cr.readUnsignedShort( offset );
            interfaces = new String[numInterfaces];
            offset += 2;
            for ( int i = 0; i < numInterfaces; i++ )
            {
                interfaces[i] = readConstantPoolString( cr, offset, buf );
                offset += 2;
            }
        }

        String readConstantPoolString( ClassReader cr, int offset, char[] buf )
        {
            int cpIndex = cr.getItem( cr.readUnsignedShort( offset ) );
            if ( cpIndex == 0 )
            {
                return null;
                // throw new RuntimeException("Bad constant pool index");
            }
            return cr.readUTF8( cpIndex, buf );
        }

        Type getType()
        {
            return type;
        }

        ClassInfo getSuperclass()
        {
            if ( superClass == null )
            {
                return null;
            }
            return new ClassInfo( superClass, loader );
        }

        /**
         * Same as {@link Class#getInterfaces()}
         */
        ClassInfo[] getInterfaces()
        {
            if ( interfaces == null )
            {
                return new ClassInfo[0];
            }
            ClassInfo[] result = new ClassInfo[interfaces.length];
            for ( int i = 0; i < result.length; ++i )
            {
                result[i] = new ClassInfo( interfaces[i], loader );
            }
            return result;
        }

        /**
         * Same as {@link Class#isInterface}
         */
        boolean isInterface()
        {
            return isInterface;
        }

        private boolean implementsInterface( ClassInfo that )
        {
            for ( ClassInfo c = this; c != null; c = c.getSuperclass() )
            {
                for ( ClassInfo iface : c.getInterfaces() )
                {
                    if ( iface.type.equals( that.type ) ||
                            iface.implementsInterface( that ) )
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isSubclassOf( ClassInfo that )
        {
            for ( ClassInfo ci = this; ci != null; ci = ci.getSuperclass() )
            {
                if ( ci.getSuperclass() != null &&
                        ci.getSuperclass().type.equals( that.type ) )
                {
                    return true;
                }
            }
            return false;
        }

        /**
         * Same as {@link Class#isAssignableFrom(Class)}
         */
        boolean isAssignableFrom( ClassInfo that )
        {
            return (this == that ||
                    that.isSubclassOf( this ) ||
                    that.implementsInterface( this ) ||
                    (that.isInterface()
                            && getType().getDescriptor().equals( "Ljava/lang/Object;" )));
        }
    }

}
