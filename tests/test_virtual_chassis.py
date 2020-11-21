import pytest
from swsscommon import swsscommon
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
            config_db = swsscommon.DBConnector(swsscommon.CONFIG_DB, dvs.redis_sock, 0)
            metatbl = swsscommon.Table(config_db, "DEVICE_METADATA")

            cfg_switch_type = ""
            status, cfg_switch_type = metatbl.hget("localhost", "switch_type")

            #Test only for line cards
            if cfg_switch_type == "voq":
                print("VOQ Switch test for {}".format(name))
                status, cfg_switch_id = metatbl.hget("localhost", "switch_id")
                assert status, "Got error in getting switch_id from CONFIG_DB DEVICE_METADATA"

                status, cfg_max_cores = metatbl.hget("localhost", "max_cores")
                assert status, "Got error in getting max_cores from CONFIG_DB DEVICE_METADATA"
                
                cfgsptbl = swsscommon.Table(config_db, "SYSTEM_PORT")
                cfgspkeys = cfgsptbl.getKeys()
                sp_count = len(cfgspkeys)

                asic_db = swsscommon.DBConnector(swsscommon.ASIC_DB, dvs.redis_sock, 0)
                tbl = swsscommon.Table(asic_db, "ASIC_STATE:SAI_OBJECT_TYPE_SWITCH")
                keys = list(tbl.getKeys())
                switch_oid_key = keys[0]

                status, value = tbl.hget(switch_oid_key, "SAI_SWITCH_ATTR_TYPE")
                assert status, "Got error while getting switch type"
                assert value == "SAI_SWITCH_TYPE_VOQ", "Switch type is not VOQ"
                
                status, value = tbl.hget(switch_oid_key, "SAI_SWITCH_ATTR_SWITCH_ID")
                assert status, "Got error while getting switch id"
                assert value == cfg_switch_id, "VOQ switch id is invalid"
                
                status, value = tbl.hget(switch_oid_key, "SAI_SWITCH_ATTR_MAX_SYSTEM_CORES")
                assert status, "Got error while getting max system cores"
                assert value == cfg_max_cores, "Max system cores is invalid"
                
                status, value = tbl.hget(switch_oid_key, "SAI_SWITCH_ATTR_SYSTEM_PORT_CONFIG_LIST")
                assert status, "Got error while getting system port config list"
                #Convert the spcfg string to dictionary
                spcfg = ast.literal_eval(value)
                assert spcfg['count'] == sp_count, "Number of systems ports configured is invalid"
    
    def test_chassis_app_db_sync(self, vct):
        """ Test chassis app db syncing """
        dvss = vct.dvss
        for name in dvss.keys():
            if name.startswith("supervisor"):
                dvs = dvss[name]
                chassis_app_db = swsscommon.DBConnector(swsscommon.CHASSIS_APP_DB, dvs.redis_chassis_sock, 0)
                tbl = swsscommon.Table(chassis_app_db, "SYSTEM_INTERFACE")
                keys = list(tbl.getKeys())
                assert len(keys), "No chassis app db syncing is done"
                
    def test_chassis_system_interface(self, vct):
        """ Test RIF record creation in ASIC_DB for remote interfaces """
        dvss = vct.dvss
        for name in dvss.keys():
            dvs = dvss[name]
            config_db = swsscommon.DBConnector(swsscommon.CONFIG_DB, dvs.redis_sock, 0)
            metatbl = swsscommon.Table(config_db, "DEVICE_METADATA")

            cfg_switch_type = ""
            status, cfg_switch_type = metatbl.hget("localhost", "switch_type")

            #Test only for line cards
            if cfg_switch_type == "voq":    
                status, lc_switch_id = metatbl.hget("localhost", "switch_id")
                assert status, "Got error in getting switch_id from CONFIG_DB DEVICE_METADATA"
                if lc_switch_id == "0":
                    #Testing in Linecard1, In Linecard1 there will be RIF for Ethernet12 from Linecard3 
                    #Note: Tesing can be done in any linecard for RIF of any system port interface.
                    #      Here testing is done on linecard with switch id 0
                    asic_db = swsscommon.DBConnector(swsscommon.ASIC_DB, dvs.redis_sock, 0)
                    riftbl = swsscommon.Table(asic_db, "ASIC_STATE:SAI_OBJECT_TYPE_ROUTER_INTERFACE")
                    keys = list(riftbl.getKeys())
                    assert len(keys), "No router interfaces in ASIC_DB"

                    rif_port_oid = ""
                    for key in keys:
                        status, value = riftbl.hget(key, "SAI_ROUTER_INTERFACE_ATTR_TYPE")
                        assert status, "Got error in getting RIF type"
                        if value == "SAI_ROUTER_INTERFACE_TYPE_PORT":
                            status, value = riftbl.hget(key, "SAI_ROUTER_INTERFACE_ATTR_PORT_ID")
                            assert status, "Got error in getting RIF port"
                            if value.startswith("oid:0x5d"):
                                #System port RIF, this is used as key for system port config info retrieval
                                rif_port_oid = value
                                break

                    assert rif_port_oid != "", "No RIF records for remote interfaces in ASIC_DB"
                    #Validate if the system port is from valid switch
                    sptbl = swsscommon.Table(asic_db, "ASIC_STATE:SAI_OBJECT_TYPE_SYSTEM_PORT")
                    status, value = sptbl.hget(rif_port_oid, "SAI_SYSTEM_PORT_ATTR_CONFIG_INFO")
                    assert status, "Got error in getting system port config info for rif system port"
                    spcfginfo = ast.literal_eval(value)
                    #Remote system ports's switch id should not match local switch id
                    assert spcfginfo["attached_switch_id"] != lc_switch_id, "RIF system port with wrong switch_id"
