import pytest
from swsscommon import swsscommon
from dvslib.dvs_database import DVSDatabase
import ast

class TestVirtualChassis(object):
    def test_connectivity(self, vct):
        if vct is None:
            return
        dvss = vct.dvss
        nbrs = vct.get_topo_neigh()
        for name in dvss.keys():
            dv = dvss[name]
            #ping all vs's inband address
            for ctn in vct.inbands.keys():
                ip = vct.inbands[ctn]["inband_address"]
                ip = ip.split("/")[0]
                print("%s: ping inband address %s" % (name, ip))
                _, out = dv.runcmd(['sh', "-c", "ping -c 5 -W 0 -q %s" % ip])
                print(out)
                assert '5 received' in out
            if name not in nbrs.keys():
                continue
            for item in nbrs[name]:
                ip = str(item[1])
                print("%s: ping neighbor address %s" % (name, ip))
                _, out = dv.runcmd(['sh', "-c", "ping -c 5 -W 0 -q %s" % ip])
                print(out)
                assert '5 received' in out

    def test_voq_switch(self, vct):
        """ Test VOQ switch objects configuration """
        dvss = vct.dvss
        for name in dvss.keys():
            dvs = dvss[name]
            #Get the config info
            config_db = DVSDatabase(swsscommon.CONFIG_DB, dvs.redis_sock)
            metatbl = config_db.get_entry("DEVICE_METADATA", "localhost")

            cfg_switch_type = metatbl.get("switch_type")

            #Test only for line cards
            if cfg_switch_type == "voq":
                print("VOQ Switch test for {}".format(name))
                cfg_switch_id = metatbl.get("switch_id")
                assert cfg_switch_id != "", "Got error in getting switch_id from CONFIG_DB DEVICE_METADATA"

                cfg_max_cores = metatbl.get("max_cores")
                assert cfg_max_cores != "", "Got error in getting max_cores from CONFIG_DB DEVICE_METADATA"
                
                cfgspkeys = config_db.get_keys("SYSTEM_PORT")
                sp_count = len(cfgspkeys)

                asic_db = DVSDatabase(swsscommon.ASIC_DB, dvs.redis_sock)
                keys = asic_db.get_keys("ASIC_STATE:SAI_OBJECT_TYPE_SWITCH")
                switch_oid_key = keys[0]
                
                switch_entry = asic_db.get_entry("ASIC_STATE:SAI_OBJECT_TYPE_SWITCH", switch_oid_key)

                value = switch_entry.get("SAI_SWITCH_ATTR_TYPE")
                assert value == "SAI_SWITCH_TYPE_VOQ", "Switch type is not VOQ"
                
                value = switch_entry.get("SAI_SWITCH_ATTR_SWITCH_ID")
                assert value == cfg_switch_id, "VOQ switch id is invalid"
                
                value = switch_entry.get("SAI_SWITCH_ATTR_MAX_SYSTEM_CORES")
                assert value == cfg_max_cores, "Max system cores is invalid"
                
                value = switch_entry.get("SAI_SWITCH_ATTR_SYSTEM_PORT_CONFIG_LIST")
                assert value != "", "Empty system port config list"
                #Convert the spcfg string to dictionary
                spcfg = ast.literal_eval(value)
                assert spcfg['count'] == sp_count, "Number of systems ports configured is invalid"
    
    def test_chassis_app_db_sync(self, vct):
        """ Test chassis app db syncing """
        dvss = vct.dvss
        for name in dvss.keys():
            if name.startswith("supervisor"):
                dvs = dvss[name]
                chassis_app_db = DVSDatabase(swsscommon.CHASSIS_APP_DB, dvs.redis_chassis_sock)
                keys = chassis_app_db.get_keys("SYSTEM_INTERFACE")
                assert len(keys), "No chassis app db syncing is done"
                
    def test_chassis_system_interface(self, vct):
        """ Test RIF record creation in ASIC_DB for remote interfaces """
        dvss = vct.dvss
        for name in dvss.keys():
            dvs = dvss[name]

            config_db = DVSDatabase(swsscommon.CONFIG_DB, dvs.redis_sock)
            metatbl = config_db.get_entry("DEVICE_METADATA", "localhost")

            cfg_switch_type = metatbl.get("switch_type")

            #Test only for line cards
            if cfg_switch_type == "voq":    
                lc_switch_id = metatbl.get("switch_id")
                assert lc_switch_id != "", "Got error in getting switch_id from CONFIG_DB DEVICE_METADATA"
                if lc_switch_id == "0":
                    #Testing in Linecard1, In Linecard1 there will be RIF for Ethernet12 from Linecard3 
                    #Note: Tesing can be done in any linecard for RIF of any system port interface.
                    #      Here testing is done on linecard with switch id 0
                    asic_db = DVSDatabase(swsscommon.ASIC_DB, dvs.redis_sock)
                    keys = asic_db.get_keys("ASIC_STATE:SAI_OBJECT_TYPE_ROUTER_INTERFACE")
                    assert len(keys), "No router interfaces in ASIC_DB"

                    rif_port_oid = ""
                    for key in keys:
                        rif_entry = asic_db.get_entry("ASIC_STATE:SAI_OBJECT_TYPE_ROUTER_INTERFACE", key)
                        value = rif_entry.get("SAI_ROUTER_INTERFACE_ATTR_TYPE")
                        assert value != "", "Got error in getting RIF type"
                        if value == "SAI_ROUTER_INTERFACE_TYPE_PORT":
                            value = rif_entry.get("SAI_ROUTER_INTERFACE_ATTR_PORT_ID")
                            assert value != "", "Got error in getting RIF port"
                            if value.startswith("oid:0x5d"):
                                #System port RIF, this is used as key for system port config info retrieval
                                rif_port_oid = value
                                break

                    assert rif_port_oid != "", "No RIF records for remote interfaces in ASIC_DB"
                    #Validate if the system port is from valid switch
                    sp_entry = asic_db.get_entry("ASIC_STATE:SAI_OBJECT_TYPE_SYSTEM_PORT", rif_port_oid)
                    value = sp_entry.get("SAI_SYSTEM_PORT_ATTR_CONFIG_INFO")
                    assert value != "", "Got error in getting system port config info for rif system port"
                    spcfginfo = ast.literal_eval(value)
                    #Remote system ports's switch id should not match local switch id
                    assert spcfginfo["attached_switch_id"] != lc_switch_id, "RIF system port with wrong switch_id"

    def test_chassis_system_neigh(self, vct):
        """ Test neigh record creation and syncing to chassis app db """
        
        """
        This test validates that:
           (i)   Local neighbor entry is created with encap index
           (ii)  Local neighbor is synced to chassis ap db with assigned encap index
           TODO: (iii) Remote neighbor entry is created in ASIC_DB with received encap index
        """

        dvss = vct.dvss
        for name in dvss.keys():
            dvs = dvss[name]

            config_db = DVSDatabase(swsscommon.CONFIG_DB, dvs.redis_sock)
            metatbl = config_db.get_entry("DEVICE_METADATA", "localhost")

            cfg_switch_type = metatbl.get("switch_type")

            #Neighbor record verifiation done in line card
            if cfg_switch_type == "voq":    
                lc_switch_id = metatbl.get("switch_id")
                assert lc_switch_id != "", "Got error in getting switch_id from CONFIG_DB DEVICE_METADATA"
                if lc_switch_id == "0":

                    # Add a static neighbor
                    _, res = dvs.runcmd(['sh', "-c", "ip neigh add 10.8.101.2 lladdr 00:01:02:03:04:05 dev Ethernet0"])
                    assert res == "", "Error configuring static neigh"

                    asic_db = DVSDatabase(swsscommon.ASIC_DB, dvs.redis_sock)
                    neighkeys = asic_db.get_keys("ASIC_STATE:SAI_OBJECT_TYPE_NEIGHBOR_ENTRY")
                    assert len(neighkeys), "No neigh entries in ASIC_DB"
                    
                    #Check for presence of the neighbor in ASIC_DB
                    test_neigh = ""
                    for nkey in neighkeys:
                        ne = ast.literal_eval(nkey)
                        if ne['ip'] == '10.8.101.2':
                            test_neigh = nkey
                            break
                        
                    assert test_neigh != "", "Neigh not found in ASIC_DB"
                    
                    #Check for presence of encap index, retrieve and store it for sync verification
                    test_neigh_entry = asic_db.get_entry("ASIC_STATE:SAI_OBJECT_TYPE_NEIGHBOR_ENTRY", test_neigh)
                    encap_index = test_neigh_entry.get("SAI_NEIGHBOR_ENTRY_ATTR_ENCAP_INDEX")
                    assert encap_index != "", "VOQ encap index is not programmed in ASIC_DB"
                    
                    break
                    
        #Verify neighbor record syncing with encap index       
        dvss = vct.dvss
        for name in dvss.keys():
            if name.startswith("supervisor"):
                dvs = dvss[name]
                chassis_app_db = DVSDatabase(swsscommon.CHASSIS_APP_DB, dvs.redis_chassis_sock)
                sysneighkeys = chassis_app_db.get_keys("SYSTEM_NEIGH")
                assert len(sysneighkeys), "No system neighbor entries in chassis app db"
                
                test_sysneigh = ""
                for sysnk in sysneighkeys:
                    sysnk_tok = sysnk.split("|")
                    assert len(sysnk_tok) == 3, "Invalid system neigh key in chassis app db"
                    if sysnk_tok[2] == "10.8.101.2":
                        test_sysneigh = sysnk
                        break
                
                assert test_sysneigh != "", "Neigh is not sync-ed to chassis app db"
               
                test_sysneigh_entry = chassis_app_db.get_entry("SYSTEM_NEIGH", test_sysneigh) 
                sys_neigh_encap_index = test_sysneigh_entry.get("encap_index")
                assert sys_neigh_encap_index != "", "System neigh in chassis app db does not have encap index"
                
                assert encap_index == sys_neigh_encap_index, "Encap index not sync-ed correctly"
                
                break

# Add Dummy always-pass test at end as workaroud
# for issue when Flaky fail on final test it invokes module tear-down before retrying
def test_nonflaky_dummy():
    pass
