/*
 * Copyright (c) YANDEX LLC, 2015. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */

#ifndef __558a6717_eede_4557_af95_a7447e1ae5ff
#define __558a6717_eede_4557_af95_a7447e1ae5ff

#include "RWSpinLock.h"

#include <vector>

class Group;

class Couple
{
public:
    enum Status {
        INIT,
        OK,
        FULL,
        BAD,
        BROKEN,
        RO,
        FROZEN,
        MIGRATING,
        SERVICE_ACTIVE,
        SERVICE_STALLED
    };

public:
    Couple();
    Couple(const std::vector<Group*> & groups);

    bool check(const std::vector<int> & groups) const;

    // no lock
    void bind_groups();

    void get_group_ids(std::vector<int> & groups) const;

    void update_status();

    const char *get_status_text() const
    { return m_status_text; }

private:
    std::vector<Group*> m_groups;
    mutable RWSpinLock m_groups_lock;

    Status m_status;
    const char *m_status_text;
};

#endif
