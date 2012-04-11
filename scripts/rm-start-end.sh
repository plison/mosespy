#! /bin/bash

#rm start-end symbols

sed 's/<s>//g' | sed 's/<\/s>//g' | sed 's/^ *//' | sed 's/ *$//' | sed '/^$/d'

