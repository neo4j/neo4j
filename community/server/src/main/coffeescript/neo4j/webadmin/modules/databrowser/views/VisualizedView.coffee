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
  ['neo4j/webadmin/modules/databrowser/visualization/VisualGraph'
   'neo4j/webadmin/modules/databrowser/DataBrowserSettings'
   'neo4j/webadmin/utils/ItemUrlResolver'
   './VisualizationSettingsDialog'
   'ribcage/View'
   'ribcage/security/HtmlEscaper'
   './visualization'
   'ribcage/ui/Dropdown'
   'neo4j/webadmin/modules/databrowser/models/DataBrowserState'],
  (VisualGraph, DataBrowserSettings, ItemUrlResolver, VisualizationSettingsDialog, View, HtmlEscaper, template, Dropdown, DataBrowserState) ->
  
    State = DataBrowserState.State

    class ProfilesDropdown extends Dropdown
      
      constructor : (@profiles, @settings) ->
        super()
      
      getItems : () ->
        items = []
        items.push @title "Profiles"
        @profiles.each (profile) =>
          items.push @actionable @renderProfileItem(profile), (ev) =>
            @settings.setCurrentVisualizationProfile profile.id
            @render()
            ev.stopPropagation()
        items.push @item "<a class='micro-button' href='#/data/visualization/settings/profile/'>New profile</a><div class='break'></div>"
        return items
        
      renderProfileItem : (profile) ->
      
        currentProfileId = @settings.getCurrentVisualizationProfile().id
        if currentProfileId == profile.id
          currentClass = 'selected' 
        else
          currentClass = ''
      
        profileButton = $ "<span class='#{currentClass}'>#{profile.getName()}</span>"
        
        if not profile.isDefault()
          editButton = $ "<a class='micro-button' href='#/data/visualization/settings/profile/#{profile.id}/'>Edit</a>"
          editButton.click @hide
          
          deleteButton = $ "<div class='bad-button micro-button'>Remove</div>"
          deleteButton.click (ev) =>
            @deleteProfile(profile)
            @render()
            ev.stopPropagation()
            
          buttons = $ "<div class='dropdown-controls'></div>"
          buttons.append editButton
          buttons.append deleteButton
          
          wrap = $ '<div></div>'
          wrap.append profileButton
          wrap.append buttons
          return wrap
        return profileButton
        
      deleteProfile : (profile) =>
        if confirm("Are you sure?")
          currentProfileId = @settings.getCurrentVisualizationProfile().id
          # Use default profile if the current
          # profile is getting removed
          if profile.id == currentProfileId
            @settings.setCurrentVisualizationProfile @profiles.first()
          @profiles.remove(profile)
          @profiles.save()
        

    class VisualizedView extends View

      events :
        'click #visualization-reflow' : "reflowGraphLayout"
        'click #visualization-profiles-button' : "showProfilesDropdown"
        'click #visualization-clear' : "clearVisualization"

      initialize : (options)->
        @server = options.server
        @appState = options.appState
        @dataModel = options.dataModel
      
        @settings = new DataBrowserSettings(@appState.getSettings())
        @dataModel.bind("change:data", @render)
        
        @settings.onCurrentVisualizationProfileChange () =>
          @getViz().setProfile @settings.getCurrentVisualizationProfile()

      render : =>
        if @vizEl? then @getViz().detach()
        $(@el).html(template())

        @vizEl = $("#visualization", @el)
        @getViz().attach(@vizEl)

        switch @dataModel.getState()
          when State.SINGLE_NODE
            @visualizeFromNode @dataModel.getData().getItem()
          when State.NODE_LIST
            @visualizeFromNodes @dataModel.getData().getRawNodes()
          when State.SINGLE_RELATIONSHIP
            @visualizeFromRelationships [@dataModel.getData().getItem()]
          when State.RELATIONSHIP_LIST
            @visualizeFromRelationships @dataModel.getData().getRawRelationships()
          when State.CYPHER_RESULT
            # TODO
            return this
          when State.EMPTY
            return this
          when State.NOT_EXECUTED
            return this
          when State.ERROR
            return this

        return this
        
      showProfilesDropdown : () ->
        @_profilesDropdown ?= new ProfilesDropdown(@settings.getVisualizationProfiles(), @settings)
        if @_profilesDropdown.isVisible()
          @_profilesDropdown.hide()
        else
          @_profilesDropdown.renderFor $("#visualization-profiles-button")

      visualizeFromNode : (node) ->
        @getViz().addNode(node)

      visualizeFromNodes : (nodes) ->
        @getViz().addNodes(nodes)

      visualizeFromRelationships : (rels) ->

        MAX = 10 # Temporary limit, should be replaced by filtering

        nodeDownloadChecklist = {}
        nodePromises = []
        
        for i in [0...rels.length]
          rel = rels[i]
          if i >= MAX
            alert "Only showing the first ten in the set, to avoid crashing the visualization. We're working on adding filtering here!"
            break
          if not nodeDownloadChecklist[rel.getStartNodeUrl()]?
            nodeDownloadChecklist[rel.getStartNodeUrl()] = true
            nodePromises.push rel.getStartNode()

          if not nodeDownloadChecklist[rel.getEndNodeUrl()]?
            nodeDownloadChecklist[rel.getStartNodeUrl()] = true
            nodePromises.push rel.getEndNode()

        allNodes = neo4j.Promise.join.apply(this, nodePromises)
        allNodes.then (nodes) =>
          @getViz().addNodes nodes
      
      getViz : () =>
        width = $(document).width() - 40;
        height = $(document).height() - 160;
        profile = @settings.getCurrentVisualizationProfile()
        @viz ?= new VisualGraph(@server, profile, width,height)
        return @viz

      reflowGraphLayout : () =>
        @viz.reflow() if @viz?
        
      clearVisualization : () =>
        @viz.clear()

      remove : =>
        @dataModel.unbind("change:data", @render)
        @getViz().stop()
        super()

      detach : =>
        @dataModel.unbind("change:data", @render)
        @getViz().stop()
        super()

      attach : (parent) =>
        super(parent)
        if @vizEl?
          @getViz().start()
          @dataModel.bind("change:data", @render)
          
)
