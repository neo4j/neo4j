###
Copyright (c) 2002-2018 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['./base',
   './bean',
   'ribcage/View',
   'lib/amd/jQuery'], 
  (baseTemplate, beanTemplate, View, $) ->
  
    class ServerInfoView extends View
      
      initialize : (options) ->
        @serverInfo = options.serverInfo

        @baseTemplate = baseTemplate
        @beanTemplate = beanTemplate

        @serverInfo.bind "change:domains", @render
        @serverInfo.bind "change:current", @renderBean

      render : =>
        $(@el).html @baseTemplate( { domains : @serverInfo.get("domains") } )
        @renderBean()
        return this

      renderBean : =>
        bean = @serverInfo.get("current")
        $("#info-bean", @el).empty().append @beanTemplate(
          bean : bean,
          attributes : if bean? then @flattenAttributes(bean.attributes) else [])
        return this

      flattenAttributes: (attributes, flattened=[], indent=1) =>
        for attr in attributes
          name = if attr.name? then attr.name else if attr.type? then attr.type else ""
          
          pushedAttr =
            name : name,
            description : attr.description,
            indent : indent
          flattened.push pushedAttr

          if not attr.value?
            pushedAttr.value = ""
          else if _(attr.value).isArray() and _(attr.value[0]).isString()
            pushedAttr.value = attr.value.join(", ") 
          else if _(attr.value).isArray()
            pushedAttr.value = ""
            @flattenAttributes(attr.value, flattened, indent + 1)
          else if typeof(attr.value) is "object"
            pushedAttr.value = ""
            @flattenAttributes(attr.value.value, flattened, indent + 1)
          else
            pushedAttr.value = attr.value

        return flattened

      remove : =>
        @serverInfo.unbind "change:domains", @render
        @serverInfo.unbind "change:current", @renderBean
        super()
)
