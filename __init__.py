#!/usr/bin/env python

from .common import VERSION

# Export version
__version__ = '.'.join('%d' % v for v in VERSION[0:3])