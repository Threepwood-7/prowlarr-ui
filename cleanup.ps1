Get-ChildItem -Path $PSScriptRoot -Directory -Recurse -Filter __pycache__ | Remove-Item -Recurse -Force
Get-ChildItem -Path $PSScriptRoot -Recurse -Filter *.log | Remove-Item -Force
