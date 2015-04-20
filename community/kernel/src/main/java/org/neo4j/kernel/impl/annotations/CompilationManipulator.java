/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Map;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;

abstract class CompilationManipulator
{
    static CompilationManipulator load( AnnotationProcessor proc, ProcessingEnvironment processingEnv )
    {
        for ( Environment env : Environment.values() )
        {
            CompilationManipulator manipulator = env.load( proc, processingEnv );
            if ( manipulator != null ) return manipulator;
        }
        return null;
    }

    private enum Environment
    {
        JAVAC( "com.sun.tools.javac.processing.JavacProcessingEnvironment" )
        {
            @Override
            CompilationManipulator create( AnnotationProcessor proc, ProcessingEnvironment env )
            {
                return new JavacManipulator( proc, env );
            }
        };
        private final Class<?> environment;

        private Environment( String environment )
        {
            this.environment = loadClass( environment );
        }

        CompilationManipulator load( AnnotationProcessor proc, ProcessingEnvironment env )
        {
            try
            {
                if ( environment != null && environment.isInstance( env ) && canLoad( env ) )
                {
                    return create( proc, env );
                }
            }
            catch ( Exception e )
            {
                return null;
            }
            catch ( LinkageError e )
            {
                return null;
            }
            return null;
        }

        boolean canLoad( @SuppressWarnings( "unused" ) ProcessingEnvironment env )
        {
            return true;
        }

        abstract CompilationManipulator create( AnnotationProcessor proc, ProcessingEnvironment env );

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
    }


    abstract boolean updateAnnotationValue( Element annotated, AnnotationMirror annotation, String key, String value );

    abstract boolean addAnnotation( Element target, String annotationType, Map<String, Object> parameters );

    @SuppressWarnings( "restriction" )
    private static class JavacManipulator extends CompilationManipulator
    {
        private final AnnotationProcessor proc;
        private final com.sun.source.util.Trees trees;
        private final com.sun.tools.javac.tree.TreeMaker maker;
        private final com.sun.tools.javac.model.JavacElements elements;

        JavacManipulator( AnnotationProcessor proc, ProcessingEnvironment env )
        {
            com.sun.tools.javac.util.Context context = ( (com.sun.tools.javac.processing.JavacProcessingEnvironment) env )
                    .getContext();
            this.proc = proc;
            this.trees = com.sun.source.util.Trees.instance( env );
            this.maker = com.sun.tools.javac.tree.TreeMaker.instance( context );
            this.elements = com.sun.tools.javac.model.JavacElements.instance( context );
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
                annot.args = annot.args.append( assignment( key, value ) );
                return true;
            }
            return false;
        }

        @Override
        boolean addAnnotation( Element target, String annotationType, Map<String, Object> parameters )
        {
            com.sun.source.tree.Tree leaf = trees.getPath( target ).getLeaf();
            final com.sun.tools.javac.tree.JCTree.JCModifiers modifiers;
            if ( leaf instanceof com.sun.tools.javac.tree.JCTree.JCMethodDecl )
            {
                com.sun.tools.javac.tree.JCTree.JCMethodDecl method = (com.sun.tools.javac.tree.JCTree.JCMethodDecl) leaf;
                modifiers = method.mods != null ? method.mods : ( method.mods = makeModifiers( target, 0 ) );
            }
            else if ( leaf instanceof com.sun.tools.javac.tree.JCTree.JCClassDecl )
            {
                com.sun.tools.javac.tree.JCTree.JCClassDecl clazz = (com.sun.tools.javac.tree.JCTree.JCClassDecl) leaf;
                modifiers = clazz.mods != null ? clazz.mods : ( clazz.mods = makeModifiers( target, 0 ) );
            }
            else if ( leaf instanceof com.sun.tools.javac.tree.JCTree.JCVariableDecl )
            {
                com.sun.tools.javac.tree.JCTree.JCVariableDecl param = (com.sun.tools.javac.tree.JCTree.JCVariableDecl) leaf;
                modifiers = param.mods != null ? param.mods : ( param.mods = makeModifiers( target, 0 ) );
            }
            else
            {
                return false;
            }
            for ( com.sun.tools.javac.tree.JCTree.JCAnnotation annotation : modifiers.annotations )
            {
                if ( annotation.annotationType instanceof com.sun.tools.javac.tree.JCTree.JCIdent )
                {
                    com.sun.tools.javac.tree.JCTree.JCIdent ident = (com.sun.tools.javac.tree.JCTree.JCIdent) annotation.annotationType;
                    if ( ident.getName().contentEquals( annotationType ) ) return false;
                }
            }
            modifiers.annotations = modifiers.annotations.prepend( maker.Annotation( typeName( annotationType ),
                    makeParams( parameters ) ) );
            return true;
        }

        private com.sun.tools.javac.tree.JCTree.JCExpression typeName( String typeName )
        {
            String[] parts = typeName.split( "\\.", -1 );
            com.sun.tools.javac.tree.JCTree.JCExpression exp = maker.Ident( elements.getName( parts[0] ) );
            for ( int i = 1; i < parts.length; i++ )
                exp = maker.Select( exp, elements.getName( parts[i] ) );
            return exp;
        }

        private com.sun.tools.javac.tree.JCTree.JCModifiers makeModifiers( Element target, long flags )
        {
            proc.warn( target, "No modifiers, creating default" );
            return maker.Modifiers( flags,
                    com.sun.tools.javac.util.List.<com.sun.tools.javac.tree.JCTree.JCAnnotation>nil() );
        }

        private com.sun.tools.javac.util.List<com.sun.tools.javac.tree.JCTree.JCExpression> makeParams(
                Map<String, Object> parameters )
        {
            com.sun.tools.javac.util.List<com.sun.tools.javac.tree.JCTree.JCExpression> result = com.sun.tools.javac.util.List
                    .<com.sun.tools.javac.tree.JCTree.JCExpression>nil();
            for ( Map.Entry<String, Object> entry : parameters.entrySet() )
            {
                result = result.prepend( assignment( entry.getKey(), entry.getValue() ) );
            }
            return result;
        }

        private com.sun.tools.javac.tree.JCTree.JCAssign assignment( String key, Object value )
        {
            return maker.Assign( maker.Ident( elements.getName( key ) ), maker.Literal( value ) );
        }
    }
}
