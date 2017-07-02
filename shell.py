#######!/usr/bin/env python
# run Django shell in SublimeREPL
# how to use: Meuns > Tools > SublimeREPL > Python > Python - RUN current file
import os

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "qsettings.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(['.', 'shell'])