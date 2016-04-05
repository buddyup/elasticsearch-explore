#!/usr/bin/env python
import sys
import explore


def reload_events(filename):
    """Reload all events, dropping the index and setting new mappings."""
    explore.drop()
    explore.create()
    explore.write_event_mapping()
    data = explore.get_data(filename)
    explore.bulk_load_events(data)


if __name__ == '__main__':
    filename = sys.argv[1]
    reload_events(filename)
