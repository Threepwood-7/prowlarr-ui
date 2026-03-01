"""Help dialog HTML content"""

HELP_HTML = """\
<h3>Prowlarr Search Client</h3>
<p>Search Prowlarr indexers and manage downloads with Everything integration.</p>
<hr>

<h4>How to Use</h4>
<ol>
<li>Type a search query and press <b>Enter</b> or click <b>Search</b></li>
<li>Filter by indexer and/or category using the tree views on the left</li>
<li>Use the page controls to navigate through result pages</li>
<li>Check <b>Hide existing</b> to hide results already found on disk (via Everything)</li>
</ol>

<h4>Downloading</h4>
<ul>
<li><b>Space</b> &ndash; Download selected row and move to next</li>
<li><b>Download Selected</b> &ndash; Download all highlighted rows (Ctrl+Click to multi-select)</li>
<li><b>Download All</b> &ndash; Download all visible (non-hidden) rows</li>
</ul>

<h4>Keyboard Shortcuts (Results Table)</h4>
<table cellpadding='4'>
<tr><td><b>Space</b></td><td>Download current row, advance to next</td></tr>
<tr><td><b>S</b></td><td>Launch Everything.exe search for the title</td></tr>
<tr><td><b>C</b></td><td>Copy release title to clipboard</td></tr>
<tr><td><b>G</b></td><td>Open web search for the title</td></tr>
<tr><td><b>P</b></td><td>Play video file found by Everything</td></tr>
<tr><td><b>F2</b></td><td>Run custom_command_F2 (config.toml)</td></tr>
<tr><td><b>F3</b></td><td>Run custom_command_F3 (config.toml)</td></tr>
<tr><td><b>F4</b></td><td>Run custom_command_F4 (config.toml)</td></tr>
<tr><td><b>Tab</b></td><td>Jump to next title group</td></tr>
<tr><td><b>Shift+Tab</b></td><td>Jump to previous title group</td></tr>
<tr><td><b>Ctrl+A</b></td><td>Select all visible rows</td></tr>
<tr><td><b>Ctrl+F</b></td><td>Find in table (Enter=next, Shift+Enter=prev, Esc=close)</td></tr>
<tr><td><b>F1</b></td><td>Show this help dialog</td></tr>
<tr><td><b>Double-click</b></td><td>Download row</td></tr>
<tr><td><b>Right-click</b></td><td>Context menu with all actions</td></tr>
</table>

<h4>Custom Commands (F2/F3/F4)</h4>
<p>Configure in <code>[settings]</code> section of config.toml:</p>
<pre>custom_command_F2 = 'my_script.bat "{title}" "{video}"'</pre>
<p>Placeholders: <b>{title}</b> = release title, <b>{video}</b> = video file path
from Everything (or empty)</p>

<h4>Everything Integration</h4>
<p>After search results load, each title is checked against Everything.
Matches turn <span style='color:gray'>gray</span> with a tooltip showing found files.
After downloading, results are rechecked automatically.</p>

<h4>Menu</h4>
<p><b>File</b></p>
<ul>

<li><b>Exit</b> &ndash; Close the application</li>
</ul>

<p><b>View</b></p>
<ul>
<li><b>Show Log</b> &ndash; Open the log window to view application messages</li>
<li><b>Download History</b> &ndash; View the log of previously downloaded items</li>
<li><b>Select Best per Group</b> &ndash; Highlight the best result in each title group based on size and seeders</li>
<li><b>Reset Sorting</b> &ndash; Restore default sort order (Title ASC, Indexer DESC, Age ASC)</li>
<li><b>Reset View</b> &ndash; Reset column widths, splitter position, and sort order to defaults</li>
</ul>
<p><b>Bookmarks</b></p>
<ul>
<li><b>Add Bookmark</b> &ndash; Save the current search query as a bookmark</li>
<li><b>Delete Bookmark</b> &ndash; Remove a saved bookmark from the list</li>
<li><b>Sort Bookmarks</b> &ndash; Sort all bookmarks alphabetically</li>
</ul>
<p><b>Help</b></p>
<ul>
<li><b>Help (F1)</b> &ndash; Show this dialog</li>
</ul>
"""
