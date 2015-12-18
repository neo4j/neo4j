# <%= version%> (<%= today%>)
<% if (_(changelog.feat).size() > 0) { %>
## Features
<% _(changelog.feat).keys().sort().forEach(function(scope) { %>
- **<%= scope%>:** <% changelog.feat[scope].forEach(function(change) { %>
  - <%= change.msg%> (<%= helpers.commitLink(change.sha1) %>)  <% }); %><% }); %> <% } %>
<% if (_(changelog.fix).size() > 0) { %>
## Bug Fixes
<% _(changelog.fix).keys().sort().forEach(function(scope) { %>
- **<%= scope%>:** <% changelog.fix[scope].forEach(function(change) { %>
  - <%= change.msg%> (<%= helpers.commitLink(change.sha1) %>)  <% }); %><% }); %> <% } %>
<% if (_(changelog.breaking).size() > 0) { %>
## Breaking Changes
<% _(changelog.breaking).keys().sort().forEach(function(scope) { %>
- **<%= scope%>:** <% changelog.breaking[scope].forEach(function(change) { %>
<%= change.msg%><% }); %><% }); %> <% } %>
