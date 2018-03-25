class ClusterDbNode:
    # requires keyword arguments (kwargs) for database name, host, and port
    # may also optionally include kwargs for partition column, two partition parameters, and a node id
    def __init__(self, *args, **kwargs):
        self.db_name = kwargs['db_name']
        self.host = kwargs['host']
        self.port = kwargs['port']
        if('part_col' in kwargs):
            self.part_col = kwargs['part_col']
        if('part_param1' in kwargs):
            self.part_param1 = kwargs['part_param1']
        if('part_param2' in kwargs):
            self.part_param2 = kwargs['part_param2']
        if('part_mtd' in kwargs):
            self.part_mtd = kwargs['part_mtd']
        if('node_id' in kwargs):
            self.node_id = kwargs['node_id']
