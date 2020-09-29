#ifndef SWSS_NEIGHORCH_H
#define SWSS_NEIGHORCH_H

#include "orch.h"
#include "observer.h"
#include "portsorch.h"
#include "intfsorch.h"

#include "ipaddress.h"
#include "nexthopkey.h"

#define NHFLAGS_IFDOWN                  0x1 // nexthop's outbound i/f is down

typedef NextHopKey NeighborEntry;

struct NextHopEntry
{
    sai_object_id_t     next_hop_id;    // next hop id
    int                 ref_count;      // reference count
    uint32_t            nh_flags;       // flags
};

/* NeighborTable: NeighborEntry, neighbor MAC address */
typedef map<NeighborEntry, MacAddress> NeighborTable;
/* NextHopTable: NextHopKey, NextHopEntry */
typedef map<NextHopKey, NextHopEntry> NextHopTable;

struct NeighborUpdate
{
    NeighborEntry entry;
    MacAddress mac;
    bool add;
};

class NeighOrch : public Orch, public Subject
{
public:
    NeighOrch(DBConnector *db, string tableName, IntfsOrch *intfsOrch, DBConnector *chassisAppDb);

    bool hasNextHop(const NextHopKey&);

    sai_object_id_t getNextHopId(const NextHopKey&);
    int getNextHopRefCount(const NextHopKey&);

    void increaseNextHopRefCount(const NextHopKey&);
    void decreaseNextHopRefCount(const NextHopKey&);

    bool getNeighborEntry(const NextHopKey&, NeighborEntry&, MacAddress&);
    bool getNeighborEntry(const IpAddress&, NeighborEntry&, MacAddress&);

    bool ifChangeInformNextHop(const string &, bool);
    bool isNextHopFlagSet(const NextHopKey &, const uint32_t);

    bool addInbandNeighbor(string alias, IpAddress ip_address);
    bool delInbandNeighbor(string alias, IpAddress ip_address);
private:
    IntfsOrch *m_intfsOrch;

    NeighborTable m_syncdNeighbors;
    NextHopTable m_syncdNextHops;

    bool addNextHop(const IpAddress&, const string&);
    bool removeNextHop(const IpAddress&, const string&);

    bool addNeighbor(const NeighborEntry&, const MacAddress&);
    bool removeNeighbor(const NeighborEntry&);

    bool setNextHopFlag(const NextHopKey &, const uint32_t);
    bool clearNextHopFlag(const NextHopKey &, const uint32_t);

    void doTask(Consumer &consumer);
    void doVoqSystemNeighTask(Consumer &consumer);

    unique_ptr<Table> m_tableVoqSystemNeighTable;
    bool getSystemPortNeighEncapIndex(string alias, uint32_t &encap_index);
    bool addVoqEncapIndex(string &alias, IpAddress &ip, vector<sai_attribute_t> &neighbor_attrs);
    bool addKernelRoute(string odev, IpAddress ip_addr);
    bool delKernelRoute(IpAddress ip_addr);
    bool addKernelNeigh(string odev, IpAddress ip_addr, MacAddress mac_addr);
    bool delKernelNeigh(string odev, IpAddress ip_addr);
    void voqSyncAddNeigh(string &alias, IpAddress &ip_address, const MacAddress &mac, sai_neighbor_entry_t &neighbor_entry);
    void voqSyncDelNeigh(string &alias, IpAddress &ip_address);
};

#endif /* SWSS_NEIGHORCH_H */
