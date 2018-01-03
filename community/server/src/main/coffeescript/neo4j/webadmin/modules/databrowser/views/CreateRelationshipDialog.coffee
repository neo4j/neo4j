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
  ['neo4j/webadmin/utils/ItemUrlResolver'
   'neo4j/webadmin/modules/databrowser/models/DataBrowserState'
   './createRelationship',
   'ribcage/View',
   'neo4j/webadmin/utils/FormHelper',
   'lib/amd/jQuery'], 
  (ItemUrlResolver, DataBrowserState, template, View, FormHelper, $) ->

    class CreateRelationshipDialog extends View

      className: "popout"

      events : 
        "click #create-relationship" : "save"
        "change #create-relationship-types" : "pickedFromAvailableTypes"

      initialize : (opts) =>
        $(@el).hide()
        $("body").append(@el)

        @formHelper = new FormHelper(@el)
        @baseElement = opts.baseElement
        @server = opts.server
        @dataModel = opts.dataModel
        @closeCallback = opts.closeCallback

        @urlResolver = new ItemUrlResolver(@server)

        @type = "RELATED_TO"
        if @dataModel.getState() is DataBrowserState.State.SINGLE_NODE
          @from = @dataModel.getData().getId()
        else
          @from = ""

        @to = ""
        
        @server.getAvailableRelationshipTypes().then (types) =>
          @types = types          
          @position()
          @render()
          $(@el).show()

      pickedFromAvailableTypes : =>
        type = $("#create-relationship-types").val()
        if type != "Types in use"
          $("#create-relationship-type").val(type)
        $("#create-relationship-types").val("Types in use")

      save : =>
        @formHelper.removeAllErrors()
        type = $("#create-relationship-type").val()
        @server.rel(@getFromUrl(), type, @getToUrl()).then @saveSuccessful, @saveFailed

      saveSuccessful : (relationship) =>
        id = @urlResolver.extractRelationshipId(relationship.getSelf())
        #@dataModel.setData( relationship, true) 
        @dataModel.setQuery( "rel:#{id}" )
        @dataModel.executeCurrentQuery()
        @closeCallback()

      saveFailed : (error) =>
        if error instanceof neo4j.exceptions.NotFoundException
          if error.url is @getFromUrl()
            @formHelper.addErrorTo("#create-relationship-from", "This node cannot be found.")
          else
            @formHelper.addErrorTo("#create-relationship-to", "This node cannot be found.")
        else
          @formHelper.addErrorTo("#create-relationship-from", "Unable to create relationship.")


      getFromUrl : ->
        @urlResolver.getNodeUrl(@urlResolver.extractNodeId($("#create-relationship-from").val()))
      
      getToUrl : ->        
        @urlResolver.getNodeUrl(@urlResolver.extractNodeId($("#create-relationship-to").val()))

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
