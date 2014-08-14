## Roadmap 

#### Directive Maintainers

Who will take the lead regarding any pull requests or decisions for a a directive?

<table width="100%">
<th>Component</th><th>Maintainer</th>
<tr>
  <td>accordion</td><td>@ajoslin</td>
</tr>
<tr>
  <td>alert</td><td>@pkozlowski</td>
</tr>
<tr>
  <td>bindHtml</td><td>frozen, use $sce?</td>
</tr>
<tr>
  <td>buttons</td><td> @pkozlowski</td>
</tr>
<tr>
  <td>carousel</td><td>@ajoslin</td>
</tr>
<tr>
  <td>collapse</td><td>$animate (@chrisirhc)</td>
</tr>
<tr>
  <td>datepicker</td><td>@bekos</td>
</tr>
<tr>
  <td>dropdownToggle</td><td>@bekos</td>
</tr>
<tr>
  <td>modal</td><td>@pkozlowski</td>
</tr>
<tr>
  <td>pagination</td><td>@bekos</td>
</tr>
<tr>
  <td>popover/tooltip</td><td>@chrisirhc</td>
</tr>
<tr>
  <td>position</td><td>@ajoslin</td>
</tr>
<tr>
  <td>progressbar</td><td>@bekos</td>
</tr>
<tr>
  <td>rating</td><td>@bekos</td>
</tr>
<tr>
  <td>tabs</td><td>@ajoslin</td>
</tr>
<tr>
  <td>timepicker</td><td>@bekos</td>
</tr>
<tr>
  <td>transition</td><td>@frozen, remove (@chrisirhc)</td>
</tr>
<tr>
  <td>typeahead</td><td>@pkozlowski, @chrisirhc</td>
</tr>
</table>


#### Attribute Prefix

Each directive should make its own two-letter prefix

`<tab tb-active=”true” tb-select=”doThis()”>`

#### Use $animate

* @chrisirhc is leading this

#### New Build system

* @ajoslin is leading this
* Building everything on travis commit
* Push to bower, nuget, cdnjs, etc

#### Switch to ngdocs

* http://github.com/petebacondarwin/angular-doc-gen

### Conventions for whether attributes/options should be watched/evaluated-once

- Boolean attributes
- Stick AngularJS conventions rather than Bootstrap conventions

