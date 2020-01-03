/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.annotations.api;

import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.StandardDoclet;
import jdk.javadoc.internal.tool.DocEnvImpl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.QualifiedNameable;

/**
 * Doclet for Neo4j javadoc generation.
 * Current doclet extends standard and filter include into final javadoc only elements that were marked with {@code PublicApi}.
 * All other elements are excluded from final javadoc.
 * In cases when an entity that marked with {@code PublicApi} references element that is not marked as part of public API - referenced class will only
 * be mentioned but not included. Meaning there will be no javadoc page for that element.
 *
 * @implNote
 * Because of limitation of {@code StandardDoclet} we need to have access to the internal part of javadoc tool. The only valid {@code DocletEnvironment}
 * that we can pass into {@link StandardDoclet#run(DocletEnvironment)} is {@link DocEnvImpl} because of internal cast inside of HtmlDoclet inside a
 * WorkArounds constructor.
 * In case if that will be fixed in future version we can implement {@link DocletEnvironment} instead of extending {@link DocEnvImpl}.
 */
public class PublicApiDoclet extends StandardDoclet
{
    @Override
    public String getName()
    {
        return "PublicApiDoclet";
    }

    @Override
    public boolean run( DocletEnvironment docEnv )
    {
        FilteringDocletEnvironment docletEnvironment = new FilteringDocletEnvironment( docEnv );
        return super.run( docletEnvironment );
    }

    private static class FilteringDocletEnvironment extends DocEnvImpl
    {
        private final DocletEnvironment docEnv;

        FilteringDocletEnvironment( DocletEnvironment docEnv )
        {
            super( ((DocEnvImpl) docEnv).toolEnv, ((DocEnvImpl) docEnv).etable );
            this.docEnv = docEnv;
        }

        @Override
        public Set<? extends Element> getIncludedElements()
        {
            Set<Element> includedElements = new HashSet<>( docEnv.getIncludedElements() );
            includedElements.removeIf( element -> !includeElement( element ) );
            return includedElements;
        }

        @Override
        public boolean isIncluded( Element e )
        {
            if ( e instanceof QualifiedNameable )
            {
                return includeElement( e );
            }
            return super.isIncluded( e );
        }

        @Override
        public boolean isSelected( Element e )
        {
            if ( e instanceof QualifiedNameable )
            {
                return includeElement( e );
            }
            return super.isIncluded( e );
        }

        private boolean includeElement( Element element )
        {
            if ( element.getAnnotation( PublicApi.class ) != null )
            {
                return true;
            }
            Element enclosingElement = element.getEnclosingElement();
            if ( enclosingElement != null && enclosingElement.getAnnotation( PublicApi.class ) != null )
            {
                return true;
            }
            if ( element instanceof PackageElement )
            {
                return includePackage( (PackageElement) element );
            }
            return false;
        }

        private boolean includePackage( PackageElement packageElement )
        {
            List<? extends Element> enclosedElements = packageElement.getEnclosedElements();
            for ( Element enclosedElement : enclosedElements )
            {
                if ( includeElement( enclosedElement ) )
                {
                    return true;
                }
            }
            return false;
        }
    }
}
