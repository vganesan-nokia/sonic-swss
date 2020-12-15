#include <assert.h>
#include "neighorch.h"
#include "logger.h"
#include "swssnet.h"
#include "crmorch.h"
#include "routeorch.h"
#include "subscriberstatetable.h"

extern sai_neighbor_api_t*         sai_neighbor_api;
extern sai_next_hop_api_t*         sai_next_hop_api;

extern PortsOrch *gPortsOrch;
extern sai_object_id_t gSwitchId;
extern CrmOrch *gCrmOrch;
extern RouteOrch *gRouteOrch;
extern FgNhgOrch *gFgNhgOrch;
extern string gMySwitchType;

const int neighorch_pri = 30;

NeighOrch::NeighOrch(DBConnector *appDb, string tableName, IntfsOrch *intfsOrch, FdbOrch *fdbOrch, PortsOrch *portsOrch, DBConnector *chassisAppDb) :
        Orch(appDb, tableName, neighorch_pri),
        m_intfsOrch(intfsOrch),
        m_fdbOrch(fdbOrch),
        m_portsOrch(portsOrch),
        m_appNeighResolveProducer(appDb, APP_NEIGH_RESOLVE_TABLE_NAME)
{
    SWSS_LOG_ENTER();

    m_fdbOrch->attach(this);
    
    if(gMySwitchType == "voq")
    {
        //Add subscriber to process VOQ system neigh
        tableName = CHASSIS_APP_SYSTEM_NEIGH_TABLE_NAME;
        Orch::addExecutor(new Consumer(new SubscriberStateTable(chassisAppDb, tableName, TableConsumable::DEFAULT_POP_BATCH_SIZE, 0), this, tableName));
        m_tableVoqSystemNeighTable = unique_ptr<Table>(new Table(chassisAppDb, CHASSIS_APP_SYSTEM_NEIGH_TABLE_NAME));

        //STATE DB connection for setting state of the remote neighbor SAI programming
        unique_ptr<DBConnector> stateDb;
        stateDb = make_unique<DBConnector>("STATE_DB", 0);
        m_stateSystemNeighTable = unique_ptr<Table>(new Table(stateDb.get(), STATE_SYSTEM_NEIGH_TABLE_NAME));
    }
}

NeighOrch::~NeighOrch()
{
    if (m_fdbOrch)
    {
        m_fdbOrch->detach(this);
    }
}

bool NeighOrch::resolveNeighborEntry(const NeighborEntry &entry, const MacAddress &mac)
{
    vector<FieldValueTuple>    data;
    IpAddress                  ip = entry.ip_address;
    string                     key, alias = entry.alias;

    key = alias + ":" + entry.ip_address.to_string();
    // We do NOT need to populate mac field as its NOT
    // even used in nbrmgr during ARP resolve. But just keeping here.
    FieldValueTuple fvTuple("mac", mac.to_string().c_str());
    data.push_back(fvTuple);

    SWSS_LOG_INFO("Flushing ARP entry '%s:%s --> %s'",
                  alias.c_str(), ip.to_string().c_str(), mac.to_string().c_str());
    m_appNeighResolveProducer.set(key, data);
    return true;
}

/*
 * Function Name: processFDBFlushUpdate
 * Description:
 *     Goal of this function is to delete neighbor/ARP entries
 * when a port belonging to a VLAN gets removed.
 * This function is called whenever neighbor orchagent receives
 * SUBJECT_TYPE_FDB_FLUSH_CHANGE notification. Currently we only care for
 * deleted FDB entries. We flush neighbor entry that matches its
 * in-coming interface and MAC with FDB entry's VLAN name and MAC
 * respectively.
 */
void NeighOrch::processFDBFlushUpdate(const FdbFlushUpdate& update)
{
    SWSS_LOG_INFO("processFDBFlushUpdate port: %s",
                    update.port.m_alias.c_str());

    for (auto entry : update.entries)
    {
        // Get Vlan object
        Port vlan;
        if (!m_portsOrch->getPort(entry.bv_id, vlan))
        {
            SWSS_LOG_NOTICE("FdbOrch notification: Failed to locate vlan port \
                             from bv_id 0x%" PRIx64 ".", entry.bv_id);
            continue;
        }
        SWSS_LOG_INFO("Flushing ARP for port: %s, VLAN: %s",
                      vlan.m_alias.c_str(), update.port.m_alias.c_str());

        // If the FDB entry MAC matches with neighbor/ARP entry MAC,
        // and ARP entry incoming interface matches with VLAN name,
        // flush neighbor/arp entry.
        for (const auto &neighborEntry : m_syncdNeighbors)
        {
            if (neighborEntry.first.alias == vlan.m_alias &&
                neighborEntry.second == entry.mac)
            {
                resolveNeighborEntry(neighborEntry.first, neighborEntry.second);
            }
        }
    }
    return;
}

void NeighOrch::update(SubjectType type, void *cntx)
{
    SWSS_LOG_ENTER();

    assert(cntx);

    switch(type) {
        case SUBJECT_TYPE_FDB_FLUSH_CHANGE:
        {
            FdbFlushUpdate *update = reinterpret_cast<FdbFlushUpdate *>(cntx);
            processFDBFlushUpdate(*update);
            break;
        }
        default:
            break;
    }

    return;
}
bool NeighOrch::hasNextHop(const NextHopKey &nexthop)
{
    return m_syncdNextHops.find(nexthop) != m_syncdNextHops.end();
}

bool NeighOrch::addNextHop(const IpAddress &ipAddress, const string &alias)
{
    SWSS_LOG_ENTER();

    Port p;
    if (!gPortsOrch->getPort(alias, p))
    {
        SWSS_LOG_ERROR("Neighbor %s seen on port %s which doesn't exist",
                        ipAddress.to_string().c_str(), alias.c_str());
        return false;
    }
    if (p.m_type == Port::SUBPORT)
    {
        if (!gPortsOrch->getPort(p.m_parent_port_id, p))
        {
            SWSS_LOG_ERROR("Neighbor %s seen on sub interface %s whose parent port doesn't exist",
                            ipAddress.to_string().c_str(), alias.c_str());
            return false;
        }
    }

    NextHopKey nexthop = { ipAddress, alias };
    if(m_intfsOrch->isRemoteSystemPortIntf(alias))
    {
        //For remote system ports kernel nexthops are always on inband. Change the key
        Port inbp;
        gPortsOrch->getInbandPort(inbp);
        assert(inbp.m_alias.length());

        nexthop.alias = inbp.m_alias;
    }
    assert(!hasNextHop(nexthop));
    sai_object_id_t rif_id = m_intfsOrch->getRouterIntfsId(alias);

    vector<sai_attribute_t> next_hop_attrs;

    sai_attribute_t next_hop_attr;
    next_hop_attr.id = SAI_NEXT_HOP_ATTR_TYPE;
    next_hop_attr.value.s32 = SAI_NEXT_HOP_TYPE_IP;
    next_hop_attrs.push_back(next_hop_attr);

    next_hop_attr.id = SAI_NEXT_HOP_ATTR_IP;
    copy(next_hop_attr.value.ipaddr, ipAddress);
    next_hop_attrs.push_back(next_hop_attr);

    next_hop_attr.id = SAI_NEXT_HOP_ATTR_ROUTER_INTERFACE_ID;
    next_hop_attr.value.oid = rif_id;
    next_hop_attrs.push_back(next_hop_attr);

    sai_object_id_t next_hop_id;
    sai_status_t status = sai_next_hop_api->create_next_hop(&next_hop_id, gSwitchId, (uint32_t)next_hop_attrs.size(), next_hop_attrs.data());
    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("Failed to create next hop %s on %s, rv:%d",
                       ipAddress.to_string().c_str(), alias.c_str(), status);
        return false;
    }

    SWSS_LOG_NOTICE("Created next hop %s on %s",
                    ipAddress.to_string().c_str(), alias.c_str());

    NextHopEntry next_hop_entry;
    next_hop_entry.next_hop_id = next_hop_id;
    next_hop_entry.ref_count = 0;
    next_hop_entry.nh_flags = 0;
    m_syncdNextHops[nexthop] = next_hop_entry;

    m_intfsOrch->increaseRouterIntfsRefCount(alias);

    if (ipAddress.isV4())
    {
        gCrmOrch->incCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEXTHOP);
    }
    else
    {
        gCrmOrch->incCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEXTHOP);
    }

    gFgNhgOrch->validNextHopInNextHopGroup(nexthop);

    // For nexthop with incoming port which has down oper status, NHFLAGS_IFDOWN
    // flag should be set on it.
    // This scenario may happen under race condition where buffered neighbor event
    // is processed after incoming port is down.
    if (p.m_oper_status == SAI_PORT_OPER_STATUS_DOWN)
    {
        if (setNextHopFlag(nexthop, NHFLAGS_IFDOWN) == false)
        {
            SWSS_LOG_WARN("Failed to set NHFLAGS_IFDOWN on nexthop %s for interface %s",
                ipAddress.to_string().c_str(), alias.c_str());
        }
    }
    return true;
}

bool NeighOrch::setNextHopFlag(const NextHopKey &nexthop, const uint32_t nh_flag)
{
    SWSS_LOG_ENTER();

    auto nhop = m_syncdNextHops.find(nexthop);
    bool rc = false;

    assert(nhop != m_syncdNextHops.end());

    if (nhop->second.nh_flags & nh_flag)
    {
        return true;
    }

    nhop->second.nh_flags |= nh_flag;

    switch (nh_flag)
    {
        case NHFLAGS_IFDOWN:
            rc = gRouteOrch->invalidnexthopinNextHopGroup(nexthop);
            break;
        default:
            assert(0);
            break;
    }

    return rc;
}

bool NeighOrch::clearNextHopFlag(const NextHopKey &nexthop, const uint32_t nh_flag)
{
    SWSS_LOG_ENTER();

    auto nhop = m_syncdNextHops.find(nexthop);
    bool rc = false;

    assert(nhop != m_syncdNextHops.end());

    if (!(nhop->second.nh_flags & nh_flag))
    {
        return true;
    }

    nhop->second.nh_flags &= ~nh_flag;

    switch (nh_flag)
    {
        case NHFLAGS_IFDOWN:
            rc = gRouteOrch->validnexthopinNextHopGroup(nexthop);
            break;
        default:
            assert(0);
            break;
    }

    return rc;
}

bool NeighOrch::isNextHopFlagSet(const NextHopKey &nexthop, const uint32_t nh_flag)
{
    SWSS_LOG_ENTER();

    auto nhop = m_syncdNextHops.find(nexthop);

    assert(nhop != m_syncdNextHops.end());

    if (nhop->second.nh_flags & nh_flag)
    {
        return true;
    }

    return false;
}

bool NeighOrch::ifChangeInformNextHop(const string &alias, bool if_up)
{
    SWSS_LOG_ENTER();
    bool rc = true;

    for (auto nhop = m_syncdNextHops.begin(); nhop != m_syncdNextHops.end(); ++nhop)
    {
        if (nhop->first.alias != alias)
        {
            continue;
        }

        if (if_up)
        {
            rc = clearNextHopFlag(nhop->first, NHFLAGS_IFDOWN);
        }
        else
        {
            rc = setNextHopFlag(nhop->first, NHFLAGS_IFDOWN);
        }

        if (rc == true)
        {
            continue;
        }
        else
        {
            break;
        }
    }

    return rc;
}

bool NeighOrch::removeNextHop(const IpAddress &ipAddress, const string &alias)
{
    SWSS_LOG_ENTER();

    NextHopKey nexthop = { ipAddress, alias };
    if(m_intfsOrch->isRemoteSystemPortIntf(alias))
    {
        //For remote system ports kernel nexthops are always on inband. Change the key
        Port inbp;
        gPortsOrch->getInbandPort(inbp);
        assert(inbp.m_alias.length());

        nexthop.alias = inbp.m_alias;
    }

    assert(hasNextHop(nexthop));

    gFgNhgOrch->invalidNextHopInNextHopGroup(nexthop);

    if (m_syncdNextHops[nexthop].ref_count > 0)
    {
        SWSS_LOG_ERROR("Failed to remove still referenced next hop %s on %s",
                       ipAddress.to_string().c_str(), alias.c_str());
        return false;
    }

    m_syncdNextHops.erase(nexthop);
    m_intfsOrch->decreaseRouterIntfsRefCount(alias);
    return true;
}

sai_object_id_t NeighOrch::getNextHopId(const NextHopKey &nexthop)
{
    assert(hasNextHop(nexthop));
    return m_syncdNextHops[nexthop].next_hop_id;
}

int NeighOrch::getNextHopRefCount(const NextHopKey &nexthop)
{
    assert(hasNextHop(nexthop));
    return m_syncdNextHops[nexthop].ref_count;
}

void NeighOrch::increaseNextHopRefCount(const NextHopKey &nexthop)
{
    assert(hasNextHop(nexthop));
    m_syncdNextHops[nexthop].ref_count ++;
}

void NeighOrch::decreaseNextHopRefCount(const NextHopKey &nexthop)
{
    assert(hasNextHop(nexthop));
    m_syncdNextHops[nexthop].ref_count --;
}

bool NeighOrch::getNeighborEntry(const NextHopKey &nexthop, NeighborEntry &neighborEntry, MacAddress &macAddress)
{
    if (!hasNextHop(nexthop))
    {
        return false;
    }

    for (const auto &entry : m_syncdNeighbors)
    {
        if (entry.first.ip_address == nexthop.ip_address && entry.first.alias == nexthop.alias)
        {
            neighborEntry = entry.first;
            macAddress = entry.second;
            return true;
        }
    }

    return false;
}

bool NeighOrch::getNeighborEntry(const IpAddress &ipAddress, NeighborEntry &neighborEntry, MacAddress &macAddress)
{
    string alias = m_intfsOrch->getRouterIntfsAlias(ipAddress);
    if (alias.empty())
    {
        return false;
    }

    NextHopKey nexthop(ipAddress, alias);
    return getNeighborEntry(nexthop, neighborEntry, macAddress);
}

void NeighOrch::doTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    if (!gPortsOrch->allPortsReady())
    {
        return;
    }

    string table_name = consumer.getTableName();
    if(table_name == CHASSIS_APP_SYSTEM_NEIGH_TABLE_NAME)
    {
        doVoqSystemNeighTask(consumer);
        return;
    }

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        KeyOpFieldsValuesTuple t = it->second;

        string key = kfvKey(t);
        string op = kfvOp(t);

        size_t found = key.find(':');
        if (found == string::npos)
        {
            SWSS_LOG_ERROR("Failed to parse key %s", key.c_str());
            it = consumer.m_toSync.erase(it);
            continue;
        }

        string alias = key.substr(0, found);

        if (alias == "eth0" || alias == "lo" || alias == "docker0")
        {
            it = consumer.m_toSync.erase(it);
            continue;
        }

        if(gPortsOrch->isInbandPort(alias))
        {
            Port ibport;
            gPortsOrch->getInbandPort(ibport);
            if(ibport.m_type != Port::VLAN)
            {
                //For "port" type Inband, the neighbors are only remote neighbors.
                //Hence, this is the neigh learned due to the kernel entry added on
                //Inband interface for the remote system port neighbors. Skip
                it = consumer.m_toSync.erase(it);
                continue;
            }
            //For "vlan" type inband, may identify the remote neighbors and skip
        }

        IpAddress ip_address(key.substr(found+1));

        NeighborEntry neighbor_entry = { ip_address, alias };

        if (op == SET_COMMAND)
        {
            Port p;
            if (!gPortsOrch->getPort(alias, p))
            {
                SWSS_LOG_INFO("Port %s doesn't exist", alias.c_str());
                it++;
                continue;
            }

            if (!p.m_rif_id)
            {
                SWSS_LOG_INFO("Router interface doesn't exist on %s", alias.c_str());
                it++;
                continue;
            }

            MacAddress mac_address;
            for (auto i = kfvFieldsValues(t).begin();
                 i  != kfvFieldsValues(t).end(); i++)
            {
                if (fvField(*i) == "neigh")
                    mac_address = MacAddress(fvValue(*i));
            }

            if (m_syncdNeighbors.find(neighbor_entry) == m_syncdNeighbors.end() || m_syncdNeighbors[neighbor_entry] != mac_address)
            {
                if (addNeighbor(neighbor_entry, mac_address))
                {
                    it = consumer.m_toSync.erase(it);
                }
                else
                {
                    it++;
                    continue;
                }
            }
            else
            {
                /* Duplicate entry */
                it = consumer.m_toSync.erase(it);
            }

            /* Remove remaining DEL operation in m_toSync for the same neighbor.
             * Since DEL operation is supposed to be executed before SET for the same neighbor
             * A remaining DEL after the SET operation means the DEL operation failed previously and should not be executed anymore
             */
            auto rit = make_reverse_iterator(it);
            while (rit != consumer.m_toSync.rend() && rit->first == key && kfvOp(rit->second) == DEL_COMMAND)
            {
                consumer.m_toSync.erase(next(rit).base());
                SWSS_LOG_NOTICE("Removed pending neighbor DEL operation for %s after SET operation", key.c_str());
            }
        }
        else if (op == DEL_COMMAND)
        {
            if (m_syncdNeighbors.find(neighbor_entry) != m_syncdNeighbors.end())
            {
                if (removeNeighbor(neighbor_entry))
                {
                    it = consumer.m_toSync.erase(it);
                }
                else
                {
                    it++;
                }
            }
            else
                /* Cannot locate the neighbor */
                it = consumer.m_toSync.erase(it);
        }
        else
        {
            SWSS_LOG_ERROR("Unknown operation type %s", op.c_str());
            it = consumer.m_toSync.erase(it);
        }
    }
}

bool NeighOrch::addNeighbor(const NeighborEntry &neighborEntry, const MacAddress &macAddress)
{
    SWSS_LOG_ENTER();

    sai_status_t status;
    IpAddress ip_address = neighborEntry.ip_address;
    string alias = neighborEntry.alias;

    sai_object_id_t rif_id = m_intfsOrch->getRouterIntfsId(alias);
    if (rif_id == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_INFO("Failed to get rif_id for %s", alias.c_str());
        return false;
    }

    sai_neighbor_entry_t neighbor_entry;
    neighbor_entry.rif_id = rif_id;
    neighbor_entry.switch_id = gSwitchId;
    copy(neighbor_entry.ip_address, ip_address);

    sai_attribute_t neighbor_attr;
    neighbor_attr.id = SAI_NEIGHBOR_ENTRY_ATTR_DST_MAC_ADDRESS;
    memcpy(neighbor_attr.value.mac, macAddress.getMac(), 6);

    if (m_syncdNeighbors.find(neighborEntry) == m_syncdNeighbors.end())
    {
        vector<sai_attribute_t> neighbor_attrs;
        neighbor_attrs.push_back(neighbor_attr);
        if(gMySwitchType == "voq")
        {
            if(!addVoqEncapIndex(alias, ip_address, neighbor_attrs))
            {
                return false;
            }
        }

        status = sai_neighbor_api->create_neighbor_entry(&neighbor_entry, static_cast<uint32_t>(neighbor_attrs.size()), neighbor_attrs.data());
        if (status != SAI_STATUS_SUCCESS)
        {
            if (status == SAI_STATUS_ITEM_ALREADY_EXISTS)
            {
                SWSS_LOG_ERROR("Entry exists: neighbor %s on %s, rv:%d",
                           macAddress.to_string().c_str(), alias.c_str(), status);
                /* Returning True so as to skip retry */
                return true;
            }
            else
            {
                SWSS_LOG_ERROR("Failed to create neighbor %s on %s, rv:%d",
                           macAddress.to_string().c_str(), alias.c_str(), status);
                return false;
            }
        }

        SWSS_LOG_NOTICE("Created neighbor %s on %s", macAddress.to_string().c_str(), alias.c_str());
        m_intfsOrch->increaseRouterIntfsRefCount(alias);

        if (neighbor_entry.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
        {
            gCrmOrch->incCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEIGHBOR);
        }
        else
        {
            gCrmOrch->incCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEIGHBOR);
        }

        if (!addNextHop(ip_address, alias))
        {
            status = sai_neighbor_api->remove_neighbor_entry(&neighbor_entry);
            if (status != SAI_STATUS_SUCCESS)
            {
                SWSS_LOG_ERROR("Failed to remove neighbor %s on %s, rv:%d",
                               macAddress.to_string().c_str(), alias.c_str(), status);
                return false;
            }
            m_intfsOrch->decreaseRouterIntfsRefCount(alias);

            if (neighbor_entry.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
            {
                gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEIGHBOR);
            }
            else
            {
                gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEIGHBOR);
            }

            return false;
        }
    }
    else
    {
        status = sai_neighbor_api->set_neighbor_entry_attribute(&neighbor_entry, &neighbor_attr);
        if (status != SAI_STATUS_SUCCESS)
        {
            SWSS_LOG_ERROR("Failed to update neighbor %s on %s, rv:%d",
                           macAddress.to_string().c_str(), alias.c_str(), status);
            return false;
        }
        SWSS_LOG_NOTICE("Updated neighbor %s on %s", macAddress.to_string().c_str(), alias.c_str());
    }

    m_syncdNeighbors[neighborEntry] = macAddress;

    NeighborUpdate update = { neighborEntry, macAddress, true };
    notify(SUBJECT_TYPE_NEIGH_CHANGE, static_cast<void *>(&update));

    if(gMySwitchType == "voq")
    {
        //Sync the neighbor to add to the CHASSIS_APP_DB
        voqSyncAddNeigh(alias, ip_address, macAddress, neighbor_entry);
    }

    return true;
}

bool NeighOrch::removeNeighbor(const NeighborEntry &neighborEntry)
{
    SWSS_LOG_ENTER();

    sai_status_t status;
    IpAddress ip_address = neighborEntry.ip_address;
    string alias = neighborEntry.alias;

    NextHopKey nexthop = { ip_address, alias };
    if(m_intfsOrch->isRemoteSystemPortIntf(alias))
    {
        //For remote system ports kernel nexthops are always on inband. Change the key
        Port inbp;
        gPortsOrch->getInbandPort(inbp);
        assert(inbp.m_alias.length());

        nexthop.alias = inbp.m_alias;
    }

    if (m_syncdNeighbors.find(neighborEntry) == m_syncdNeighbors.end())
    {
        return true;
    }

    if (m_syncdNextHops[nexthop].ref_count > 0)
    {
        SWSS_LOG_INFO("Failed to remove still referenced neighbor %s on %s",
                      m_syncdNeighbors[neighborEntry].to_string().c_str(), alias.c_str());
        return false;
    }

    sai_object_id_t rif_id = m_intfsOrch->getRouterIntfsId(alias);

    sai_neighbor_entry_t neighbor_entry;
    neighbor_entry.rif_id = rif_id;
    neighbor_entry.switch_id = gSwitchId;
    copy(neighbor_entry.ip_address, ip_address);

    sai_object_id_t next_hop_id = m_syncdNextHops[nexthop].next_hop_id;
    status = sai_next_hop_api->remove_next_hop(next_hop_id);
    if (status != SAI_STATUS_SUCCESS)
    {
        /* When next hop is not found, we continue to remove neighbor entry. */
        if (status == SAI_STATUS_ITEM_NOT_FOUND)
        {
            SWSS_LOG_ERROR("Failed to locate next hop %s on %s, rv:%d",
                           ip_address.to_string().c_str(), alias.c_str(), status);
        }
        else
        {
            SWSS_LOG_ERROR("Failed to remove next hop %s on %s, rv:%d",
                           ip_address.to_string().c_str(), alias.c_str(), status);
            return false;
        }
    }

    if (status != SAI_STATUS_ITEM_NOT_FOUND)
    {
        if (neighbor_entry.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
        {
            gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEXTHOP);
        }
        else
        {
            gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEXTHOP);
        }
    }

    SWSS_LOG_NOTICE("Removed next hop %s on %s",
                    ip_address.to_string().c_str(), alias.c_str());

    status = sai_neighbor_api->remove_neighbor_entry(&neighbor_entry);
    if (status != SAI_STATUS_SUCCESS)
    {
        if (status == SAI_STATUS_ITEM_NOT_FOUND)
        {
            SWSS_LOG_ERROR("Failed to locate neigbor %s on %s, rv:%d",
                    m_syncdNeighbors[neighborEntry].to_string().c_str(), alias.c_str(), status);
            return true;
        }
        else
        {
            SWSS_LOG_ERROR("Failed to remove neighbor %s on %s, rv:%d",
                    m_syncdNeighbors[neighborEntry].to_string().c_str(), alias.c_str(), status);
            return false;
        }
    }

    if (neighbor_entry.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
    {
        gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEIGHBOR);
    }
    else
    {
        gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEIGHBOR);
    }

    SWSS_LOG_NOTICE("Removed neighbor %s on %s",
            m_syncdNeighbors[neighborEntry].to_string().c_str(), alias.c_str());

    m_syncdNeighbors.erase(neighborEntry);
    m_intfsOrch->decreaseRouterIntfsRefCount(alias);

    NeighborUpdate update = { neighborEntry, MacAddress(), false };
    notify(SUBJECT_TYPE_NEIGH_CHANGE, static_cast<void *>(&update));

    removeNextHop(ip_address, alias);

    if(gMySwitchType == "voq")
    {
        //Sync the neighbor to delete from the CHASSIS_APP_DB
        voqSyncDelNeigh(alias, ip_address);
    }

    return true;
}

void NeighOrch::doVoqSystemNeighTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    //Local inband port as the outgoing interface of the static neighbor and static route
    Port ibif;
    if(!gPortsOrch->getInbandPort(ibif))
    {
        //Inband port is not ready yet.
        return;
    }

    // For "port" type inband interface, wait till the Inband interface is both admin up and oper up
    if (ibif.m_type != Port::VLAN)
    {
        if (ibif.m_admin_state_up != true || ibif.m_oper_status != SAI_PORT_OPER_STATUS_UP)
        {
            // Inband port is not operational yet
            return;
        }
    }

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        KeyOpFieldsValuesTuple t = it->second;
        string key = kfvKey(t);
        string op = kfvOp(t);

        size_t found = key.find_last_of(consumer.getConsumerTable()->getTableNameSeparator().c_str());
        if (found == string::npos)
        {
            SWSS_LOG_ERROR("Failed to parse key %s", key.c_str());
            it = consumer.m_toSync.erase(it);
            continue;
        }

        string alias = key.substr(0, found);

        if(!gIntfsOrch->isRemoteSystemPortIntf(alias))
        {
            //Synced local neighbor. Skip
            it = consumer.m_toSync.erase(it);
            continue;
        }

        IpAddress ip_address(key.substr(found+1));

        NeighborEntry neighbor_entry = { ip_address, alias };

        string state_key = alias + state_db_key_delimiter + ip_address.to_string();

        if (op == SET_COMMAND)
        {
            Port p;
            if (!gPortsOrch->getPort(alias, p))
            {
                SWSS_LOG_INFO("Port %s doesn't exist", alias.c_str());
                it++;
                continue;
            }

            if (!p.m_rif_id)
            {
                SWSS_LOG_INFO("Router interface doesn't exist on %s", alias.c_str());
                it++;
                continue;
            }

            MacAddress mac_address;
            uint32_t encap_index = 0;
            for (auto i = kfvFieldsValues(t).begin();
                 i  != kfvFieldsValues(t).end(); i++)
            {
                if (fvField(*i) == "neigh")
                    mac_address = MacAddress(fvValue(*i));

                if(fvField(*i) == "encap_index")
                {
                    encap_index = (uint32_t)stoul(fvValue(*i));
                }
            }

            if(!encap_index)
            {
                //Encap index is not available yet. Since this is remote neighbor, we need to wait till
                //Encap index is made available either by dynamic syncing or by static config
                it++;
                continue;
            }

            if (m_syncdNeighbors.find(neighbor_entry) == m_syncdNeighbors.end() || m_syncdNeighbors[neighbor_entry] != mac_address)
            {
                //Add neigh to SAI
                if (addNeighbor(neighbor_entry, mac_address))
                {
                    //neigh successfully added to SAI. Set STATE DB to signal kernel programming by neighbor manager

                    //If the inband interface type is not VLAN, same MAC can be used for the inband interface for
                    //kernel programming.
                    if(ibif.m_type != Port::VLAN)
                    {
                        mac_address = gMacAddress;
                    }
                    vector<FieldValueTuple> fvVector;
                    FieldValueTuple mac("neigh", mac_address.to_string());
                    fvVector.push_back(mac);
                    m_stateSystemNeighTable->set(state_key, fvVector);

                    it = consumer.m_toSync.erase(it);
                }
                else
                {
                    SWSS_LOG_ERROR("Failed to add voq neighbor %s to SAI", kfvKey(t).c_str());
                    it++;
                }
            }
            else
            {
                /* Duplicate entry */
                SWSS_LOG_INFO("System neighbor %s already exists", kfvKey(t).c_str());
                it = consumer.m_toSync.erase(it);
            }
        }
        else if (op == DEL_COMMAND)
        {
            if (m_syncdNeighbors.find(neighbor_entry) != m_syncdNeighbors.end())
            {
                //Remove neigh from SAI
                if (removeNeighbor(neighbor_entry))
                {
                    //neigh successfully deleted from SAI. Set STATE DB to signal to remove entries from kernel
                    m_stateSystemNeighTable->del(state_key);

                    it = consumer.m_toSync.erase(it);
                }
                else
                {
                    SWSS_LOG_ERROR("Failed to remove voq neighbor %s from SAI", kfvKey(t).c_str());
                    it++;
                }
            }
            else
                /* Cannot locate the neighbor */
                it = consumer.m_toSync.erase(it);
        }
        else
        {
            SWSS_LOG_ERROR("Unknown operation type %s", op.c_str());
            it = consumer.m_toSync.erase(it);
        }
    }
}

bool NeighOrch::addInbandNeighbor(string alias, IpAddress ip_address)
{
    //Add neighbor record in SAI without adding host route for local inband to avoid route
    //looping for packets destined to the Inband interface if the Inband is port type

    if(gIntfsOrch->isRemoteSystemPortIntf(alias))
    {
        //Remote Inband interface. Skip
        return true;
    }

    sai_status_t status;
    MacAddress inband_mac = gMacAddress;

    sai_object_id_t rif_id = gIntfsOrch->getRouterIntfsId(alias);
    if (rif_id == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_INFO("Failed to get rif_id for %s", alias.c_str());
        return false;
    }

    //Make the object key
    sai_neighbor_entry_t neighbor_entry;
    neighbor_entry.rif_id = rif_id;
    neighbor_entry.switch_id = gSwitchId;
    copy(neighbor_entry.ip_address, ip_address);

    vector<sai_attribute_t> neighbor_attrs;
    sai_attribute_t attr;
    attr.id = SAI_NEIGHBOR_ENTRY_ATTR_DST_MAC_ADDRESS;
    memcpy(attr.value.mac, inband_mac.getMac(), 6);
    neighbor_attrs.push_back(attr);

    if(!addVoqEncapIndex(alias, ip_address, neighbor_attrs))
    {
        return false;
    }

    //No host route for neighbor of the Inband IP address
    attr.id = SAI_NEIGHBOR_ENTRY_ATTR_NO_HOST_ROUTE;
    attr.value.booldata = true;
    neighbor_attrs.push_back(attr);

    status = sai_neighbor_api->create_neighbor_entry(&neighbor_entry, static_cast<uint32_t>(neighbor_attrs.size()), neighbor_attrs.data());
    if (status != SAI_STATUS_SUCCESS)
    {
        if (status == SAI_STATUS_ITEM_ALREADY_EXISTS)
        {
            SWSS_LOG_ERROR("Entry exists: neighbor %s on %s, rv:%d", inband_mac.to_string().c_str(), alias.c_str(), status);
            return true;
        }
        else
        {
            SWSS_LOG_ERROR("Failed to create neighbor %s on %s, rv:%d", inband_mac.to_string().c_str(), alias.c_str(), status);
            return false;
        }
    }

    SWSS_LOG_NOTICE("Created inband neighbor %s on %s", inband_mac.to_string().c_str(), alias.c_str());

    gIntfsOrch->increaseRouterIntfsRefCount(alias);

    if (neighbor_entry.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
    {
        gCrmOrch->incCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEIGHBOR);
    }
    else
    {
        gCrmOrch->incCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEIGHBOR);
    }

    //Sync the neighbor to add to the CHASSIS_APP_DB
    voqSyncAddNeigh(alias, ip_address, inband_mac, neighbor_entry);

    return true;
}

bool NeighOrch::delInbandNeighbor(string alias, IpAddress ip_address)
{
    //Remove neighbor from SAI
    if(gIntfsOrch->isRemoteSystemPortIntf(alias))
    {
        //Remote Inband interface. Skip
        return true;
    }

    MacAddress inband_mac = gMacAddress;

    sai_object_id_t rif_id = gIntfsOrch->getRouterIntfsId(alias);

    sai_neighbor_entry_t neighbor_entry;
    neighbor_entry.rif_id = rif_id;
    neighbor_entry.switch_id = gSwitchId;
    copy(neighbor_entry.ip_address, ip_address);

    sai_status_t status;
    status = sai_neighbor_api->remove_neighbor_entry(&neighbor_entry);
    if (status != SAI_STATUS_SUCCESS)
    {
        if (status == SAI_STATUS_ITEM_NOT_FOUND)
        {
            SWSS_LOG_ERROR("Failed to locate neigbor %s on %s, rv:%d", inband_mac.to_string().c_str(), alias.c_str(), status);
            return true;
        }
        else
        {
            SWSS_LOG_ERROR("Failed to remove neighbor %s on %s, rv:%d", inband_mac.to_string().c_str(), alias.c_str(), status);
            return false;
        }
    }

    if (neighbor_entry.ip_address.addr_family == SAI_IP_ADDR_FAMILY_IPV4)
    {
        gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV4_NEIGHBOR);
    }
    else
    {
        gCrmOrch->decCrmResUsedCounter(CrmResourceType::CRM_IPV6_NEIGHBOR);
    }

    SWSS_LOG_NOTICE("Removed neighbor %s on %s", inband_mac.to_string().c_str(), alias.c_str());

    gIntfsOrch->decreaseRouterIntfsRefCount(alias);

    //Sync the neighbor to delete from the CHASSIS_APP_DB
    voqSyncDelNeigh(alias, ip_address);

    return true;
}

bool NeighOrch::getSystemPortNeighEncapIndex(string &alias, IpAddress &ip, uint32_t &encap_index)
{
    string value;
    string key = alias + m_tableVoqSystemNeighTable->getTableNameSeparator().c_str() + ip.to_string();

    if(m_tableVoqSystemNeighTable->hget(key, "encap_index", value))
    {
        encap_index = (uint32_t) stoul(value);
        return true;
    }
    return false;
}

bool NeighOrch::addVoqEncapIndex(string &alias, IpAddress &ip, vector<sai_attribute_t> &neighbor_attrs)
{
    sai_attribute_t attr;
    uint32_t encap_index = 0;

    if(gIntfsOrch->isRemoteSystemPortIntf(alias))
    {
        if(getSystemPortNeighEncapIndex(alias, ip, encap_index))
        {
            attr.id = SAI_NEIGHBOR_ENTRY_ATTR_ENCAP_INDEX;
            attr.value.u32 = encap_index;
            neighbor_attrs.push_back(attr);

            attr.id = SAI_NEIGHBOR_ENTRY_ATTR_ENCAP_IMPOSE_INDEX;
            attr.value.booldata = true;
            neighbor_attrs.push_back(attr);

            attr.id = SAI_NEIGHBOR_ENTRY_ATTR_IS_LOCAL;
            attr.value.booldata = false;
            neighbor_attrs.push_back(attr);
        }
        else
        {
            //Encap index not available and the interface is remote. Return false to re-try
            SWSS_LOG_NOTICE("System port neigh encap index not available for %s|%s!", alias.c_str(), ip.to_string().c_str());
            return false;
        }
    }

    return true;
}

void NeighOrch::voqSyncAddNeigh(string &alias, IpAddress &ip_address, const MacAddress &mac, sai_neighbor_entry_t &neighbor_entry)
{
    sai_attribute_t attr;
    sai_status_t status;

    //Sync only local neigh. Confirm for the local neigh and
    //get the system port alias for key for syncing to CHASSIS_APP_DB
    Port port;
    if(gPortsOrch->getPort(alias, port))
    {
        if(port.m_system_port_info.type == SAI_SYSTEM_PORT_TYPE_REMOTE)
        {
            return;
        }
        alias = port.m_system_port_info.alias;
    }
    else
    {
        SWSS_LOG_ERROR("Port does not exist for %s!", alias.c_str());
        return;
    }

    attr.id = SAI_NEIGHBOR_ENTRY_ATTR_ENCAP_INDEX;

    status = sai_neighbor_api->get_neighbor_entry_attribute(&neighbor_entry, 1, &attr);
    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("Failed to get neighbor attribute for %s on %s, rv:%d", ip_address.to_string().c_str(), alias.c_str(), status);
        return;
    }

    if (!attr.value.u32)
    {
        SWSS_LOG_ERROR("Invalid neighbor encap_index for %s on %s", ip_address.to_string().c_str(), alias.c_str());
        return;
    }

    vector<FieldValueTuple> attrs;

    FieldValueTuple eiFv ("encap_index", to_string(attr.value.u32));
    attrs.push_back(eiFv);

    FieldValueTuple macFv ("neigh", mac.to_string());
    attrs.push_back(macFv);

    string key = alias + m_tableVoqSystemNeighTable->getTableNameSeparator().c_str() + ip_address.to_string();
    m_tableVoqSystemNeighTable->set(key, attrs);
}

void NeighOrch::voqSyncDelNeigh(string &alias, IpAddress &ip_address)
{
    //Sync only local neigh. Confirm for the local neigh and
    //get the system port alias for key for syncing to CHASSIS_APP_DB
    Port port;
    if(gPortsOrch->getPort(alias, port))
    {
        if(port.m_system_port_info.type == SAI_SYSTEM_PORT_TYPE_REMOTE)
        {
            return;
        }
        alias = port.m_system_port_info.alias;
    }
    else
    {
        SWSS_LOG_ERROR("Port does not exist for %s!", alias.c_str());
        return;
    }

    string key = alias + m_tableVoqSystemNeighTable->getTableNameSeparator().c_str() + ip_address.to_string();
    m_tableVoqSystemNeighTable->del(key);
}
