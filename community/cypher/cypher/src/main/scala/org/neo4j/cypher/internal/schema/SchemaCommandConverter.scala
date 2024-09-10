/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.schema

import org.eclipse.collections.api.factory.Lists
import org.eclipse.collections.api.factory.Sets
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateFulltextIndex
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateSingleLabelPropertyIndex
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.NodePropertyExistence
import org.neo4j.cypher.internal.ast.NodePropertyUniqueness
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.PointCreateIndex
import org.neo4j.cypher.internal.ast.RangeCreateIndex
import org.neo4j.cypher.internal.ast.RelationshipPropertyExistence
import org.neo4j.cypher.internal.ast.RelationshipPropertyUniqueness
import org.neo4j.cypher.internal.ast.TextCreateIndex
import org.neo4j.cypher.internal.ast.VectorCreateIndex
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.optionsmap.CreateFulltextIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateIndexProviderOnlyOptions
import org.neo4j.cypher.internal.optionsmap.CreateIndexWithFullOptions
import org.neo4j.cypher.internal.optionsmap.CreateLookupIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreatePointIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateRangeIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateTextIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.CreateVectorIndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.IndexBackedConstraintsOptionsConverter
import org.neo4j.cypher.internal.optionsmap.IndexOptionsConverter
import org.neo4j.cypher.internal.optionsmap.PropertyExistenceOrTypeConstraintOptionsConverter
import org.neo4j.cypher.internal.procs.PropertyTypeMapper
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.schema.AllIndexProviderDescriptors.providerDescriptorDetails
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.SchemaCommand
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipExistence
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipKey
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipPropertyType
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipUniqueness
import org.neo4j.internal.schema.SchemaCommand.IndexCommand
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeFulltext
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeLookup
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodePoint
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeRange
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeText
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeVector
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipFulltext
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipLookup
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipPoint
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipRange
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipText
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipVector
import org.neo4j.internal.schema.SchemaCommand.SchemaCommandReaderException
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException
import org.neo4j.values.virtual.MapValue

import java.util

import scala.collection.Seq
import scala.jdk.CollectionConverters.IterableHasAsJava

class SchemaCommandConverter(
  private val cypherVersion: CypherVersion,
  private val latestVectorIndexVersion: VectorIndexVersion
) {

  private val ERROR_SUFFIX = " in import schema commands."

  private val providerContext: IndexProviderContext = (_, providerString: String, indexType: IndexType) => {
    val details = providerDescriptorDetails(providerString).orElseThrow(() =>
      new IndexProviderNotFoundException("Unable to find the IndexProviderDescriptor for the name: " + providerString)
    )
    val actualType = details.`type`
    if (actualType != indexType) throw new SchemaCommandReaderException(
      "The provider '%s' of type %s does not match the expected type of %s".formatted(
        providerString,
        actualType,
        indexType
      )
    )
    details.descriptor
  }

  @throws[SchemaCommandReaderException]
  def apply(command: org.neo4j.cypher.internal.ast.SchemaCommand): SchemaCommand = command match {
    case DropConstraintOnName(name, ifExists, None) =>
      new ConstraintCommand.Drop(checkName(name, "constraint name"), ifExists);
    case DropIndexOnName(name, ifExists, None) => new IndexCommand.Drop(checkName(name, "index name"), ifExists);
    case CreateLookupIndex(_, isNodeIndex, _, indexName, indexType, ifExistsDo, options) =>
      val desc = if (isNodeIndex) indexType.nodeDescription else indexType.relDescription
      val name = indexName.map(n => checkName(n, desc + " name")).orNull
      val notExists = ifNotExists(ifExistsDo)
      val config = providerOnly(validateOptions(options, CreateLookupIndexOptionsConverter(providerContext)))
      if (isNodeIndex) new NodeLookup(name, notExists, config)
      else new RelationshipLookup(name, notExists, config)
    case index @ CreateFulltextIndex(_, entityNames, properties, indexName, _, ifExistsDo, options) =>
      val config = validateOptions(options, CreateFulltextIndexOptionsConverter(providerContext))
      val desc = index.entityIndexDescription
      val name = indexName.map(n => checkName(n, desc + " name")).orNull
      val props = setLikeList(properties.map((p: Property) => p.propertyKey.name), desc, "property")
      entityNames match {
        case Left(labels) =>
          new NodeFulltext(
            name,
            setLikeList(labels.map(l => l.name), desc, "label"),
            props,
            ifNotExists(ifExistsDo),
            providerWithConfig(config),
            indexConfig(config)
          )
        case Right(types) => new RelationshipFulltext(
            name,
            setLikeList(types.map(t => t.name), desc, "relationship"),
            props,
            ifNotExists(ifExistsDo),
            providerWithConfig(config),
            indexConfig(config)
          )
      }
    case index @ CreateSingleLabelPropertyIndex(
        _,
        elementName,
        properties,
        indexName,
        indexType,
        ifExistsDo,
        options
      ) =>
      val desc = index.entityIndexDescription
      val isNode = index.isNodeIndex
      val name = indexName.map(n => checkName(n, desc + " name")).orNull
      val entityName = tokenName(elementName)
      indexType match {
        case _: RangeCreateIndex =>
          val props = setLikeList(properties.map((p: Property) => p.propertyKey.name), desc, "property")
          val config = validateOptions(options, CreateRangeIndexOptionsConverter(desc, providerContext))
          if (isNode) {
            new NodeRange(
              name,
              entityName,
              props,
              ifNotExists(ifExistsDo),
              providerOnly(config)
            )
          } else {
            new RelationshipRange(
              name,
              entityName,
              props,
              ifNotExists(ifExistsDo),
              providerOnly(config)
            )
          }
        case TextCreateIndex =>
          val config = validateOptions(options, CreateTextIndexOptionsConverter(providerContext))
          if (isNode) {
            new NodeText(
              name,
              entityName,
              singleProperty(properties),
              ifNotExists(ifExistsDo),
              providerOnly(config)
            )
          } else {
            new RelationshipText(
              name,
              entityName,
              singleProperty(properties),
              ifNotExists(ifExistsDo),
              providerOnly(config)
            )
          }
        case PointCreateIndex =>
          val config = validateOptions(options, CreatePointIndexOptionsConverter(providerContext))
          if (isNode) {
            new NodePoint(
              name,
              entityName,
              singleProperty(properties),
              ifNotExists(ifExistsDo),
              providerWithConfig(config),
              indexConfig(config)
            )
          } else {
            new RelationshipPoint(
              name,
              entityName,
              singleProperty(properties),
              ifNotExists(ifExistsDo),
              providerWithConfig(config),
              indexConfig(config)
            )
          }
        case VectorCreateIndex =>
          val config =
            validateOptions(options, CreateVectorIndexOptionsConverter(providerContext, latestVectorIndexVersion))
          if (isNode) {
            new NodeVector(
              name,
              entityName,
              singleProperty(properties),
              ifNotExists(ifExistsDo),
              providerWithConfig(config),
              indexConfig(config)
            )
          } else {
            new RelationshipVector(
              name,
              entityName,
              singleProperty(properties),
              ifNotExists(ifExistsDo),
              providerWithConfig(config),
              indexConfig(config)
            )
          }
        case _ =>
          throw new SchemaCommandReaderException("Unrecognised index change found: " + command.getClass.getSimpleName)
      }
    case CreateConstraint(_, elementName, properties, constraintName, constraintType, ifExistsDo, options) =>
      val desc = constraintType.description
      val name = constraintName.map(n => checkName(n, desc + " name")).orNull
      val entityName = tokenName(elementName)
      constraintType match {
        case org.neo4j.cypher.internal.ast.NodeKey =>
          val config = validateOptions(options, IndexBackedConstraintsOptionsConverter("range index", providerContext))
          new NodeKey(
            name,
            entityName,
            asList(properties.map(p => p.propertyKey.name)),
            ifNotExists(ifExistsDo),
            providerOnly(config)
          )
        case org.neo4j.cypher.internal.ast.RelationshipKey =>
          val config = validateOptions(options, IndexBackedConstraintsOptionsConverter("range index", providerContext))
          new RelationshipKey(
            name,
            entityName,
            asList(properties.map(p => p.propertyKey.name)),
            ifNotExists(ifExistsDo),
            providerOnly(config)
          )
        case NodePropertyUniqueness =>
          val config = validateOptions(options, IndexBackedConstraintsOptionsConverter("range index", providerContext))
          new NodeUniqueness(
            name,
            entityName,
            asList(properties.map(p => p.propertyKey.name)),
            ifNotExists(ifExistsDo),
            providerOnly(config)
          )
        case RelationshipPropertyUniqueness =>
          val config = validateOptions(options, IndexBackedConstraintsOptionsConverter("range index", providerContext))
          new RelationshipUniqueness(
            name,
            entityName,
            asList(properties.map(p => p.propertyKey.name)),
            ifNotExists(ifExistsDo),
            providerOnly(config)
          )
        case NodePropertyExistence =>
          validateOptions(
            options,
            PropertyExistenceOrTypeConstraintOptionsConverter("node", "existence", providerContext)
          )
          new NodeExistence(name, entityName, singleProperty(properties), ifNotExists(ifExistsDo))
        case RelationshipPropertyExistence =>
          validateOptions(
            options,
            PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "existence", providerContext)
          )
          new RelationshipExistence(name, entityName, singleProperty(properties), ifNotExists(ifExistsDo))
        case org.neo4j.cypher.internal.ast.NodePropertyType(propType) =>
          validateOptions(options, PropertyExistenceOrTypeConstraintOptionsConverter("node", "type", providerContext))
          new NodePropertyType(
            name,
            entityName,
            singleProperty(properties),
            PropertyTypeMapper.asPropertyTypeSet(propType),
            ifNotExists(ifExistsDo)
          )
        case org.neo4j.cypher.internal.ast.RelationshipPropertyType(propType) =>
          validateOptions(
            options,
            PropertyExistenceOrTypeConstraintOptionsConverter("relationship", "type", providerContext)
          )
          new RelationshipPropertyType(
            name,
            entityName,
            singleProperty(properties),
            PropertyTypeMapper.asPropertyTypeSet(propType),
            ifNotExists(ifExistsDo)
          )
      }
    case _ =>
      throw new SchemaCommandReaderException("Unrecognised schema change found: " + command.getClass.getSimpleName)
  }

  @throws[SchemaCommandReaderException]
  private def checkName(name: Either[String, Parameter], message: String): String = name
    .swap
    .getOrElse(throw new SchemaCommandReaderException("Parameters are not allowed to be used as a %s%s".formatted(
      message,
      ERROR_SUFFIX
    )))

  @throws[SchemaCommandReaderException]
  private def tokenName(name: ElementTypeName): String =
    name match {
      case LabelName(n)   => n
      case RelTypeName(n) => n
      case _              => throw new SchemaCommandReaderException("Unrecognised entity token type: " + name)
    }

  private def ifNotExists(ifExistsDo: IfExistsDo) = ifExistsDo eq IfExistsDoNothing

  private def providerOnly(providerOptions: Option[CreateIndexProviderOnlyOptions])
    : util.Optional[IndexProviderDescriptor] =
    util.Optional.ofNullable(providerOptions.flatMap(opts => opts.provider).orNull)

  private def providerWithConfig(providerOptions: Option[CreateIndexWithFullOptions])
    : util.Optional[IndexProviderDescriptor] =
    util.Optional.ofNullable(providerOptions.flatMap(opts => opts.provider).orNull)

  private def indexConfig(providerOptions: Option[CreateIndexWithFullOptions]): IndexConfig =
    providerOptions.map(opts => opts.config).orElse(Option.apply(IndexConfig.empty())).get

  @throws[SchemaCommandReaderException]
  private def validateOptions[OPTION](options: Options, converter: IndexOptionsConverter[OPTION]): Option[OPTION] = {
    if (options.isInstanceOf[OptionsParam])
      throw new SchemaCommandReaderException("Parameterised options are not allowed" + ERROR_SUFFIX)
    converter.convert(cypherVersion, options, MapValue.EMPTY, Option.empty)
  }

  private def asList[TYPE](list: Seq[TYPE]): util.List[TYPE] =
    Lists.mutable.withAll(list.asJava)

  @throws[SchemaCommandReaderException]
  private def setLikeList[TYPE](
    list: scala.collection.immutable.List[TYPE],
    desc: String,
    token: String
  ): util.List[TYPE] = {
    val values = Lists.mutable.empty[TYPE]
    val unique = Sets.mutable.empty[TYPE]
    for (value <- list) {
      if (!unique.add(value))
        throw new SchemaCommandReaderException("Invalid %s as %s '%s' is duplicated".formatted(
          desc,
          token,
          value
        ))
      values.add(value)
    }
    values
  }

  private def singleProperty(properties: Seq[Property]): String =
    singleItem(properties, (p: Property) => p.propertyKey.name)

  private def singleItem[TYPE, RESULT](seq: Seq[TYPE], mapper: TYPE => RESULT): RESULT = {
    try {
      Iterables.single(seq.map(mapper).asJava)
    } catch {
      case ex: Throwable => throw new SchemaCommandReaderException("Expected only a single property", ex);
    }
  }
}
