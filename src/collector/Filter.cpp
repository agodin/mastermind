/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#include "Filter.h"

#include <algorithm>

void Filter::sort()
{
#define SORT(v) \
    std::sort(v.begin(), v.end()); \
    auto v## _it = std::unique(v.begin(), v.end()); \
    v.erase(v## _it, v.end())

    SORT(namespaces);
    SORT(couples);
    SORT(groups);
    SORT(backends);
    SORT(nodes);
    SORT(filesystems);
}

bool Filter::empty() const
{
    return namespaces.empty() &&
        couples.empty() &&
        groups.empty() &&
        backends.empty() &&
        nodes.empty() &&
        filesystems.empty();
}
