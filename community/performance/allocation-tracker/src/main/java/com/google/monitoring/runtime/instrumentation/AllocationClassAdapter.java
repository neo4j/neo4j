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
// Copyright 2009 Google Inc. All Rights Reserved.

package com.google.monitoring.runtime.instrumentation;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.commons.JSRInlinerAdapter;

/**
 * Instruments bytecodes that allocate heap memory to call a recording hook.
 * A <code>ClassVisitor</code> that processes methods with a
 * <code>AllocationMethodAdapter</code> to instrument heap allocations.
 *
 * @author jeremymanson@google.com (Jeremy Manson)
 * @author fischman@google.com (Ami Fischman) (Original Author)
 */
class AllocationClassAdapter extends ClassVisitor
{
    private final String recorderClass;
    private final String recorderMethod;

    public AllocationClassAdapter( ClassVisitor cv, String recorderClass,
                                   String recorderMethod )
    {
        super( Opcodes.ASM4, cv );
        this.recorderClass = recorderClass;
        this.recorderMethod = recorderMethod;
    }

    /**
     * For each method in the class being instrumented, <code>visitMethod</code>
     * is called and the returned MethodVisitor is used to visit the method.
     * Note that a new MethodVisitor is constructed for each method.
     */
    @Override
    public MethodVisitor visitMethod( int access, String base, String desc,
                                      String signature, String[] exceptions )
    {
        MethodVisitor mv =
                cv.visitMethod( access, base, desc, signature, exceptions );

        if ( mv != null )
        {
            // We need to compute stackmaps (see
            // AllocationInstrumenter#instrument).  This can't really be
            // done for old bytecode that contains JSR and RET instructions.
            // So, we remove JSRs and RETs.
            JSRInlinerAdapter jsria = new JSRInlinerAdapter(
                    mv, access, base, desc, signature, exceptions );
            AllocationMethodAdapter aimv =
                    new AllocationMethodAdapter( jsria, recorderClass, recorderMethod );
            LocalVariablesSorter lvs = new LocalVariablesSorter( access, desc, aimv );
            aimv.lvs = lvs;
            mv = lvs;
        }
        return mv;
    }
}
