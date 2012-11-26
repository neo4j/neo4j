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

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.CodeSizeEvaluator;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a class writer that gets used in place of the existing
 * {@link ClassWriter}, and verifies properties of the class getting written.
 * <p/>
 * Currently, it only checks to see if the methods are of the correct length
 * for Java methods (<64K).
 *
 * @author jeremymanson@google.com (Jeremy Manson)
 */
public class VerifyingClassAdapter extends ClassVisitor
{
    private static final Logger logger =
            Logger.getLogger( VerifyingClassAdapter.class.getName() );

    /**
     * An enum which indicates whether the class in question is verified.
     */
    public enum State
    {
        PASS, UNKNOWN, FAIL_TOO_LONG;
    }

    final ClassWriter cw;
    final byte[] original;
    final String className;
    String message;
    State state;

    /**
     * @param cw        A class writer that is wrapped by this class adapter
     * @param original  the original bytecode
     * @param className the name of the class being examined.
     */
    public VerifyingClassAdapter( ClassWriter cw, byte[] original,
                                  String className )
    {
        super( Opcodes.ASM4, cw );
        state = State.UNKNOWN;
        message = "The class has not finished being examined";
        this.cw = cw;
        this.original = original;
        this.className = className.replace( '/', '.' );
    }

    /**
     * {@inheritDoc}
     * <p/>
     * In addition, the returned {@link MethodVisitor} will throw an exception
     * if the method is greater than 64K in length.
     */
    @Override
    public MethodVisitor visitMethod(
            final int access,
            final String name,
            final String desc,
            final String signature,
            final String[] exceptions )
    {
        MethodVisitor mv =
                super.visitMethod( access, name, desc, signature, exceptions );
        return new CodeSizeEvaluator( mv )
        {
            @Override
            public void visitEnd()
            {
                super.visitEnd();
                if ( getMaxSize() > 64 * 1024 )
                {
                    state = State.FAIL_TOO_LONG;
                    message = "the method " + name + " was too long.";
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visitEnd()
    {
        super.visitEnd();
        if ( state == State.UNKNOWN )
        {
            state = State.PASS;
        }
    }

    /**
     * Gets the verification state of this class.
     *
     * @return true iff the class passed inspection.
     */
    public boolean isVerified()
    {
        return state == State.PASS;
    }

    /**
     * Returns the byte array that contains the byte code for this class.
     *
     * @return a byte array.
     */
    public byte[] toByteArray()
    {
        if ( state != State.PASS )
        {
            logger.log( Level.WARNING,
                    "Failed to instrument class " + className + " because " + message );
            return original;
        }
        return cw.toByteArray();
    }
}
