###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/data/ItemUrlResolver'
   'neo4j/webadmin/templates/databrowser/createRelationship',
   'lib/backbone'], 
  (ItemUrlResolver, template) ->
  
    class CreateRelationshipDialog extends Backbone.View

      className: "create-relationship-dialog"

      events : 
        "click #create-relationship" : "save"
        "change #create-relationship-types" : "pickedFromAvailableTypes"

      initialize : (opts) =>
        $("body").append(@el)

        @baseElement = opts.baseElement
        @server = opts.server
        @dataModel = opts.dataModel
        @closeCallback = opts.closeCallback

        @urlResolver = new ItemUrlResolver(@server)

        @type = "RELATED_TO"
        if @dataModel.dataIsSingleNode()
          @from = @dataModel.getData().getId()
        else
          @from = ""

        @to = ""
        
        @server.getAvailableRelationshipTypes().then (types) =>
          @types = types          
          @render()
          @position()

      pickedFromAvailableTypes : =>
        type = $("#create-relationship-types").val()
        if type != "Types in use"
          $("#create-relationship-type").val(type)
        $("#create-relationship-types").val("Types in use")

      save : =>
        type = $("#create-relationship-type").val()
        fromId = @urlResolver.extractNodeId($("#create-relationship-from").val())
        toId = @urlResolver.extractNodeId($("#create-relationship-to").val())

        fromUrl = @urlResolver.getNodeUrl(fromId)
        toUrl = @urlResolver.getNodeUrl(toId)

        @server.rel(fromUrl, type, toUrl).then (relationship) =>
          id = @urlResolver.extractRelationshipId(relationship.getSelf())
          @dataModel.setData( relationship, true, {silent:true} ) 
          @dataModel.setQuery( "rel:#{id}", true)
          @closeCallback()

      position : =>
        basePos = $(@baseElement).offset()
        top = basePos.top + $(@baseElement).outerHeight()

        left= basePos.left - ($(@el).outerWidth()-$(@baseElement).outerWidth())
  
        $(@el).css({position:"absolute", top:top+"px", left:left+"px"})
        

      render : () =>
        $(@el).html(template(
          from : @from
          to   : @to
          type : @type
          types : @types
        ))
        return this

)
