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

#include "Backend.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Metrics.h"
#include "Namespace.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

#include <algorithm>

Couple::Couple(Storage & storage, const std::vector<Group*> & groups)
    :
    m_storage(storage),
    m_status(INIT),
    m_status_text(""),
    m_update_status_time(0)
{
    m_groups = groups;
}

Couple::Couple(Storage & storage)
    :
    m_storage(storage),
    m_status(INIT),
    m_status_text(""),
    m_update_status_time(0)
{}

void Couple::clone_from(const Couple & other)
{
    merge(other);
}

bool Couple::check(const std::vector<int> & groups) const
{
    if (m_groups.size() != groups.size())
        return false;
    for (size_t i = 0; i < m_groups.size(); ++i)
        if (m_groups[i]->get_id() != groups[i])
            return false;

    return true;
}

void Couple::bind_groups()
{
    m_key.clear();
    for (size_t i = 0; i < m_groups.size(); ++i) {
        m_groups[i]->set_couple(this);
        m_key += std::to_string(m_groups[i]->get_id());
        if (i != (m_groups.size() - 1))
            m_key += ':';
    }
}

void Couple::get_group_ids(std::vector<int> & groups) const
{
    groups.reserve(m_groups.size());
    for (size_t i = 0; i < m_groups.size(); ++i)
        groups.push_back(m_groups[i]->get_id());
}

void Couple::get_groups(std::vector<Group*> & groups) const
{
    groups.assign(m_groups.begin(), m_groups.end());
}

void Couple::update_status()
{
    Stopwatch watch(m_update_status_time);

    if (m_groups.empty()) {
        m_status = BAD;
        m_status_text = "Couple has no groups";
        return;
    }

    std::vector<Group::Status> statuses;

    Group *g = m_groups[0];
    statuses.push_back(g->get_status());

    bool have_frozen = g->get_frozen();

    for (size_t i = 1; i < m_groups.size(); ++i) {
        if (!g->check_metadata_equals(*m_groups[i])) {
            m_status = BAD;
            m_status_text = "Groups have different metadata";
            return;
        }

        statuses.push_back(m_groups[i]->get_status());
        if (m_groups[i]->get_frozen())
            have_frozen = true;
    }

    if (have_frozen) {
        m_status = FROZEN;
        m_status_text = "Some groups are frozen";
        return;
    }

    if (size_t(std::count(statuses.begin(), statuses.end(), Group::COUPLED)) == statuses.size()) {
        if (m_storage.get_app().get_config().forbidden_unmatched_group_total_space) {
            for (size_t i = 1; i < m_groups.size(); ++i) {
                if (m_groups[i]->get_total_space() != m_groups[0]->get_total_space()) {
                    m_status = BROKEN;
                    m_status_text = "Couple has unequal total space in groups";
                }
            }
        }

        bool full = false;
        for (Group *group : m_groups) {
            if (group->full()) {
                full = true;
                break;
            }
        }

        if (full) {
            m_status = FULL;
            m_status_text = "Couple is FULL";
        } else {
            m_status = OK;
            m_status_text = "Couple is OK";
        }
        return;
    }

    for (size_t i = 0; i < statuses.size(); ++i) {
        Group::Status status = statuses[i];
        if (status == Group::INIT) {
            m_status = INIT;
            m_status_text = "Some groups are uninitialized";
        } else if (status == Group::BAD) {
            m_status = BAD;
            m_status_text = "Some groups are in state BAD";
        } else if (status == Group::BROKEN) {
            m_status = BROKEN;
            m_status_text = "Some groups are in state BROKEN";
        } else if (status == Group::RO || status == Group::MIGRATING) {
            m_status = BAD;
            m_status_text = "Some groups are read-only";
        }
    }

    m_status = BAD;
    m_status_text = "Couple is BAD for unknown reason";

    // TODO: account job
}

void Couple::merge(const Couple & other)
{
    if (m_groups.size() != other.m_groups.size()) {
        BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                "Couple merge: internal inconsistency: different number of groups");
    }

    // XXX timestamp
    m_status = other.m_status;
    m_status_text = other.m_status_text;
    m_update_status_time = other.m_update_status_time;
}

void Couple::print_info(std::ostream & ostr) const
{
    ostr << "Couple {\n"
            "  key: " << m_key << "\n"
            "  groups: [ ";
    for (Group *group : m_groups)
        ostr << group->get_id() << ' ';
    ostr << "]\n"
            "  status: " << status_str(m_status) << "\n"
            "  status_text: '" << m_status_text << "'\n"
            "}";
}

void Couple::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("groups");
    writer.StartArray();
    for (Group *group : m_groups)
        writer.Uint64(group->get_id());
    writer.EndArray();

    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("status_text");
    writer.String(m_status_text);

    writer.EndObject();
}

const char *Couple::status_str(Status status)
{
    switch (status)
    {
    case INIT:
        return "INIT";
    case OK:
        return "OK";
    case FULL:
        return "FULL";
    case BAD:
        return "BAD";
    case BROKEN:
        return "BROKEN";
    case RO:
        return "RO";
    case FROZEN:
        return "FROZEN";
    case MIGRATING:
        return "MIGRATING";
    case SERVICE_ACTIVE:
        return "SERVICE_ACTIVE";
    case SERVICE_STALLED:
        return "SERVICE_STALLED";
    }
    return "UNKNOWN";
}