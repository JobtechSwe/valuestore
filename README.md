# Valuestore Modulen
Pythonmodul för värdeförråd

## Användning

Den här modulen är inte tänkt att köras standalone, utan som ett berorende från applikationer som behöver slå i värdeförråd. Ofta vill man dock utveckla denna modul samtidigt som "huvudapplikationen" och för att göra det behöver man först aktivera den virtuella miljö som tillhör huvudapplikationen och sedan installera modulen i develop-läge. 

Exempel virtualenv:

    $ workon <virtuell miljö för huvudapplikationen>
    $ python setup.py develop

Exempel anaconde

    $ source activate <virtuell miljö för huvudapplikationen>
    $ python setup.py develop

