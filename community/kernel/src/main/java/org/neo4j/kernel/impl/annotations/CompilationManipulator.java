/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.annotations;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;

abstract class CompilationManipulator
{
    private static final Class<?> JAVAC_ENVIRONMENT;
    static
    {
        JAVAC_ENVIRONMENT = loadClass( "com.sun.tools.javac.processing.JavacProcessingEnvironment" );
    }

    static CompilationManipulator load( ProcessingEnvironment processingEnv )
    {
        try
        {
            if ( JAVAC_ENVIRONMENT != null && JAVAC_ENVIRONMENT.isInstance( processingEnv ) )
            {
                return new JavacManipulator( processingEnv );
            }
        }
        catch ( Exception e )
        {
            // move on to next
        }
        return null;
    }

    private static Class<?> loadClass( String className )
    {
        try
        {
            return Class.forName( className );
        }
        catch ( Throwable e )
        {
            return null;
        }
    }

    abstract boolean updateAnnotationValue( Element annotated, AnnotationMirror annotation, String key, String value );

    @SuppressWarnings( "restriction" )
    private static class JavacManipulator extends CompilationManipulator
    {
        private final com.sun.source.util.Trees trees;
        private final com.sun.tools.javac.tree.TreeMaker maker;
        private final com.sun.tools.javac.model.JavacElements elements;

        JavacManipulator( ProcessingEnvironment env )
        {
            com.sun.tools.javac.util.Context context = ( (com.sun.tools.javac.processing.JavacProcessingEnvironment) env )
                    .getContext();
            trees = com.sun.source.util.Trees.instance( env );
            maker = com.sun.tools.javac.tree.TreeMaker.instance( context );
            elements = com.sun.tools.javac.model.JavacElements.instance( context );
        }

        @Override
        boolean updateAnnotationValue( Element annotated, AnnotationMirror annotation, String key, String value )
        {
            com.sun.source.tree.Tree leaf = trees.getTree( annotated, annotation );
            if ( leaf instanceof com.sun.tools.javac.tree.JCTree.JCAnnotation )
            {
                com.sun.tools.javac.tree.JCTree.JCAnnotation annot = (com.sun.tools.javac.tree.JCTree.JCAnnotation) leaf;
                for ( com.sun.tools.javac.tree.JCTree.JCExpression expr : annot.args )
                {
                    if ( expr instanceof com.sun.tools.javac.tree.JCTree.JCAssign )
                    {
                        com.sun.tools.javac.tree.JCTree.JCAssign assign = (com.sun.tools.javac.tree.JCTree.JCAssign) expr;
                        if ( assign.lhs instanceof com.sun.tools.javac.tree.JCTree.JCIdent )
                        {
                            com.sun.tools.javac.tree.JCTree.JCIdent ident = (com.sun.tools.javac.tree.JCTree.JCIdent) assign.lhs;
                            if ( ident.name.contentEquals( key ) )
                            {
                                assign.rhs = maker.Literal( value );
                                return true;
                            }
                        }
                    }
                }
                annot.args = annot.args.append( maker.Assign( maker.Ident( elements.getName( key ) ),
                        maker.Literal( value ) ) );
                return true;
            }
            return false;
        }
    }
}
