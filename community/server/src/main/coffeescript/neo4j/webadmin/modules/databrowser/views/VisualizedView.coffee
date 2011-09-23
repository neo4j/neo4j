###
Copyright (c) 2002-2011 "Neo Technology,"
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
   'ribcage/ui/Dropdown'],
  (VisualGraph, DataBrowserSettings, ItemUrlResolver, VisualizationSettingsDialog, View, HtmlEscaper, template, Dropdown) ->

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

      initialize : (options)->
        @server = options.server
        @appState = options.appState
        @dataModel = options.dataModel
      
        @settings = new DataBrowserSettings(@appState.getSettings())
        @settings.labelPropertiesChanged @settingsChanged
        @dataModel.bind("change:data", @render)
        
        @settings.onCurrentVisualizationProfileChange () =>
          @getViz().setProfile @settings.getCurrentVisualizationProfile()

      render : =>
        if @browserHasRequiredFeatures()
          if @vizEl? then @getViz().detach()
          $(@el).html(template())

          @vizEl = $("#visualization", @el)
          @getViz().attach(@vizEl)

          switch @dataModel.get("type")
            when "node"
              @visualizeFromNode @dataModel.getData().getItem()
            when "nodeList"
              @visualizeFromNodes @dataModel.getData().getRawNodes()
            when "relationship"
              @visualizeFromRelationships [@dataModel.getData().getItem()]
            when "relationshipList"
              @visualizeFromRelationships @dataModel.getData().getRawRelationships()
        else 
          @showBrowserNotSupportedMessage()

        return this
        
      showProfilesDropdown : () ->
        @_profilesDropdown ?= new ProfilesDropdown(@settings.getVisualizationProfiles(), @settings)
        if @_profilesDropdown.isVisible()
          @_profilesDropdown.hide()
        else
          @_profilesDropdown.renderFor $("#visualization-profiles-button")

      visualizeFromNode : (node) ->
        @getViz().setNode(node)

      visualizeFromNodes : (nodes) ->
        @getViz().setNodes(nodes)

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
          @getViz().setNodes nodes

      settingsChanged : () =>
        if @viz?
          @viz.setLabelProperties(@settings.getLabelProperties())
      
      getViz : () =>
        width = $(document).width() - 40;
        height = $(document).height() - 160;
        profile = @settings.getCurrentVisualizationProfile()
        @viz ?= new VisualGraph(@server, profile, width,height)
        @settingsChanged()
        return @viz

      browserHasRequiredFeatures : ->
        Object.prototype.__defineGetter__?

      showBrowserNotSupportedMessage : ->
        $(@el).html """<div class='pad'>
          <h1>I currently do not support visualization in this browser :(</h1>
          <p>I can't find the __defineGetter__ API method, which the visualization lib I use, Arbor.js, needs.</p>
          <p>If you really want to use visualization (it's pretty awesome), please consider using Google Chrome, Firefox or Safari.</p>
          </div>""" 
      
      reflowGraphLayout : () =>
        @viz.reflow() if @viz?

      remove : =>
        if @browserHasRequiredFeatures()
          @dataModel.unbind("change:data", @render)
          @getViz().stop()
        super()

      detach : =>
        if @browserHasRequiredFeatures()
          @dataModel.unbind("change:data", @render)
          @getViz().stop()
        super()

      attach : (parent) =>
        super(parent)
        if @browserHasRequiredFeatures() and @vizEl?
          @getViz().start()
          @dataModel.bind("change:data", @render)
          
)
