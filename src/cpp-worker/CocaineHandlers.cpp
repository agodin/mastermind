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

#include "CocaineHandlers.h"
#include "Couple.h"
#include "FS.h"
#include "Group.h"
#include "Node.h"
#include "Storage.h"

void on_summary::on_chunk(const char *chunk, size_t size)
{
    Storage & storage = m_app.get_storage();

    std::vector<Node*> nodes;
    storage.get_nodes(nodes);

    size_t nr_backends = 0;
    for (size_t i = 0; i < nodes.size(); ++i)
        nr_backends += nodes[i]->get_backend_count();

    std::vector<Group*> groups;
    storage.get_groups(groups);

    std::vector<Couple*> couples;
    storage.get_couples(couples);

    std::vector<FS*> filesystems;
    storage.get_filesystems(filesystems);

    std::map<Group::Status, int> group_status;
    for (size_t i = 0; i < groups.size(); ++i)
        ++group_status[groups[i]->get_status()];

    std::map<Couple::Status, int> couple_status;
    for (size_t i = 0; i < couples.size(); ++i)
        ++couple_status[couples[i]->get_status()];

    std::map<FS::Status, int> fs_status;
    for (size_t i = 0; i < filesystems.size(); ++i)
        ++fs_status[filesystems[i]->get_status()];

    std::ostringstream ostr;

    ostr << "Storage contains:\n"
         << nodes.size() << " nodes\n";

    ostr << filesystems.size() << " filesystems\n  ( ";
    for (auto it = fs_status.begin(); it != fs_status.end(); ++it)
        ostr << it->second << ' ' << FS::status_str(it->first) << ' ';

    ostr << ")\n" << nr_backends << " backends\n";

    ostr << groups.size() << " groups\n  ( ";
    for (auto it = group_status.begin(); it != group_status.end(); ++it)
        ostr << it->second << ' ' << Group::status_str(it->first) << ' ';

    ostr << ")\n" << couples.size() << " couples\n  ( ";
    for (auto it = couple_status.begin(); it != couple_status.end(); ++it)
        ostr << it->second << ' ' << Couple::status_str(it->first) << ' ';
    ostr << ")\n";

    std::vector<Namespace*> namespaces;
    storage.get_namespaces(namespaces);

    ostr << namespaces.size() << " namespaces\n";

    response()->write(ostr.str());
}

void on_group_info::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;

    do {
        char c;
        int group_id;
        if (sscanf(chunk, "%d%c", &group_id, &c) != 1) {
            ostr << "Invalid group id " << group_id;
            break;
        }

        Group *group;
        if (!m_app.get_storage().get_group(group_id, group)) {
            ostr << "Group " << group_id << " is not found";
            break;
        }

        group->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
}

void on_list_nodes::on_chunk(const char *chunk, size_t size)
{
    std::vector<Node*> nodes;
    m_app.get_storage().get_nodes(nodes);

    std::ostringstream ostr;
    ostr << "There are " << nodes.size() << " nodes\n";

    for (size_t i = 0; i < nodes.size(); ++i) {
        Node *node = nodes[i];
        if (node == NULL) {
            ostr << "  <NULL>\n";
            continue;
        }
        ostr << "  " << node->get_host() << ':' << node->get_port()
             << ':' << node->get_family() << '\n';
    }

    response()->write(ostr.str());
}

void on_node_info::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;
    do {
        Node *node;
        if (!m_app.get_storage().get_node(chunk, node)) {
            ostr << "Node " << chunk << " does not exist";
            break;
        }

        node->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
}

void on_node_list_backends::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;
    do {
        Node *node;
        if (!m_app.get_storage().get_node(chunk, node)) {
            ostr << "Node " << chunk << " does not exist";
            break;
        }

        if (node == NULL) {
            ostr << "Node is NULL";
            break;
        }

        std::vector<BackendStat*> backends;
        node->get_backends(backends);

        ostr << "Node has " << backends.size() << " backends\n";

        std::string key = chunk;
        key += '/';
        for (size_t i = 0; i < backends.size(); ++i)
            ostr << "  " << key + std::to_string(backends[i]->backend_id) << '\n';
    } while (0);

    response()->write(ostr.str());
}

void on_backend_info::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;

    do {
        const char *slash = std::strchr(chunk, '/');
        if (slash == NULL) {
            ostr << "Invalid backend id '" << chunk << "'\n"
                    "Syntax: <host>:<port>:<family>/<backend id>";
            break;
        }

        std::string node_name(chunk, slash - chunk);
        int backend_id = atoi(slash + 1);

        Node *node;
        if (!m_app.get_storage().get_node(node_name, node)) {
            ostr << "Node " << node_name << " does not exist";
            break;
        }

        BackendStat *backend;
        if (!node->get_backend(backend_id, backend)) {
            ostr << "Backend " << backend_id << " does not exist";
            break;
        }

        backend->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
}

void on_fs_info::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;

    do {
        FS *fs;
        if (!m_app.get_storage().get_fs(chunk, fs)) {
            ostr << "Found no FS '" << chunk << '\'';
            break;
        }

        fs->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
}

void on_fs_list_backends::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;

    do {
        FS *fs;
        if (!m_app.get_storage().get_fs(chunk, fs)) {
            ostr << "Found no FS '" << chunk << '\'';
            break;
        }

        std::vector<BackendStat*> backends;
        fs->get_backends(backends);

        ostr << "There are " << backends.size() << " backends\n";
        if (backends.empty())
            break;

        for (size_t i = 0; i < backends.size(); ++i) {
            const BackendStat *stat = backends[i];
            ostr << "  " << stat->node->get_key() << '/' << stat->backend_id << '\n';
        }
    } while (0);

    response()->write(ostr.str());
}
