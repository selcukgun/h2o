import sys, unittest, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import2 as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod 
    def tearDownClass(cls): 
        h2o.tear_down_cloud()
 
    def test_small_parse_sequential_same_dest_del(self):
        csvFilename = 'poker-hand-testing.data'
        csvPathname = 'poker/' + csvFilename
        for trials in range(100):
            src_key = csvPathname
            hex_key = csvPathname + '.hex'
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put',
                src_key=src_key, hex_key=hex_key, timeoutSecs=180, noPoll=False, doSummary=False)
            h2o.nodes[0].remove_key(src_key)
            h2o.nodes[0].remove_key(hex_key)

if __name__ == "__main__":
	h2o.unit_main()
