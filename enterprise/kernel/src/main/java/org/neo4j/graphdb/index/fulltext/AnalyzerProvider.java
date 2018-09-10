/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.graphdb.index.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.util.NoSuchElementException;

import org.neo4j.helpers.Service;

/**
 * This is the base-class for all service-loadable factory classes, that build the Lucene Analyzer instances that are available to the fulltext schema index.
 * The analyzer factory is referenced in the index configuration via its {@code analyzerName} and {@code alternativeNames} that are specific to the constructor
 * of this base class. Sub-classes must have a public no-arg constructor such that they can be service-loaded.
 * <p>
 * Here is an example that implements an analyzer provider for the {@code SwedishAnalyzer} that comes built into Lucene:
 *
 * <pre><code>
 * public class Swedish extends AnalyzerProvider
 * {
 *     public Swedish()
 *     {
 *         super( "swedish" );
 *     }
 *
 *     public Analyzer createAnalyzer()
 *     {
 *         return new SwedishAnalyzer();
 *     }
 * }
 * </code></pre>
 * <p>
 * The {@code jar} that includes this implementation must also contain a {@code META-INF/services/org.neo4j.graphdb.index.fulltext.AnalyzerProvider} file,
 * that contains the fully-qualified class names of all of the {@code AnalyzerProvider} implementations it contains.
 */
public abstract class AnalyzerProvider extends Service
{
    /**
     * Sub-classes MUST have a public no-arg constructor, and must call this super-constructor with the names it uses to identify itself.
     * <p>
     * Sub-classes should strive to make these names unique.
     * If the names are not unique among all analyzer providers on the class path, then the indexes may fail to load the correct analyzers that they are
     * configured with.
     *
     * @param analyzerName The name of this analyzer provider, which will be used for analyzer settings values for identifying which implementation to use.
     * @param alternativeNames The alternative names that can also be used to identify this analyzer provider.
     */
    public AnalyzerProvider( String analyzerName, String... alternativeNames )
    {
        super( analyzerName, alternativeNames );
    }

    public static AnalyzerProvider getProviderByName( String analyzerName ) throws NoSuchElementException
    {
        return Service.load( AnalyzerProvider.class, analyzerName );
    }

    /**
     * @return A newly constructed {@code Analyzer} instance.
     */
    public abstract Analyzer createAnalyzer();
}
