package org.apache.zookeeper.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ZookeeperTest {
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private CreateMode createMode;
    private ZooKeeper client;
    private static ZooKeeperServer server;
    private static int port;
    private Type type;
    private int version;
    private enum Type{
        CREATE_KEEPEREX,
        CREATE,
        CREATE_ILLEGALARGEX,
        CREATE_KEEPEREF,
        DELETE_KEEPERNONODE,
        DELETE_KEEPERBADVER,
        DELETE_KEEPERNOEMPTY,
        DELETE
    }
    @BeforeClass
    public static void createServer() throws IOException, InterruptedException {
        try {
            int tickTime = 2000;
            int numConnections = 5000;
            String dataDirectory = System.getProperty("java.io.tmpdir");

            File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
            ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
            ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(12345, numConnections);
            int zkPort = standaloneServerFactory.getLocalPort();
            port = zkPort;
            standaloneServerFactory.startup(server);
            ZookeeperTest.server = server;
        }catch(Exception e){

        }
    }

    public ZookeeperTest(String path, byte[] data, List<ACL> acl, CreateMode createMode,Type type, int version) throws IOException {
        String connection = "127.0.0.1:12345";
        System.out.println(connection);
        this.client = new ZooKeeper(connection, 2000, event -> {
            //do something with the event processed
        });
        this.path = path;
        this.data = data;
        this.acl = acl;
        this.createMode = createMode;
        this.type = type;
        this.version = version;
    }
    @Parameterized.Parameters
    public static Collection configure(){
        byte[] data = {};
        return Arrays.asList(new Object[][] {
                {"/test","2010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, Type.CREATE,0},
                {"/test3","2010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.CREATOR_ALL_ACL,CreateMode.EPHEMERAL,Type.CREATE,0},
                {"/test2","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.READ_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL,Type.CREATE,0},
                {"/testfail/no/parent",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_KEEPEREX,0},
                {"\\kestfail/\\ladpath",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_ILLEGALARGEX,0},
                {"/provaeph",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_KEEPEREF,0},
                {"/test_no_node",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERNONODE,0},
                {"/test_bad_version",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERBADVER,2},
                {"/test_bad_version2",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERBADVER,-2},
                {"/test_delete",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE,0},
                {"/test_delete_children",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERNOEMPTY,0}
        });
    }
    @Test
    public void testCreate() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.CREATE);
        byte[] data = {};
        String pathReturned = this.client.create(this.path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
        boolean correctPath = pathReturned.startsWith(this.path);
        Assert.assertTrue(correctPath);
    }
    @Test(expected = KeeperException.NoNodeException.class)
    public void testCreateKeeperEx() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.CREATE_KEEPEREX);
        String pathReturned = this.client.create(this.path,this.data, this.acl,this.createMode);
    }
    @Test(expected = KeeperException.NoChildrenForEphemeralsException.class)
    public void testCreateKeeperExEphemeral() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.CREATE_KEEPEREF);
        byte[] parentData = {};
        String pathParent = this.client.create("/ephemeral_parent6",parentData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        this.client.create("/ephemeral_parent6/new_node_fail",this.data,this.acl, this.createMode);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testCreateIllegalArgEx() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.CREATE_ILLEGALARGEX);
        this.client.create(this.path, this.data, this.acl, this.createMode);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testDeleteKeeperEx() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.DELETE_KEEPERNONODE);
        this.client.delete(this.path,this.version);
    }

    @Test(expected = KeeperException.BadVersionException.class)
    public void testDeleteBadVersion() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.DELETE_KEEPERBADVER);
        try {
            this.client.create(this.path, this.data, this.acl, this.createMode);
        }catch(KeeperException.NodeExistsException e){
            //donothing
        }
        this.client.delete(this.path,this.version);
    }

    @Test(expected = KeeperException.NotEmptyException.class)
    public void testDeleteWithChildren() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.DELETE_KEEPERNOEMPTY);
        boolean children_create= false;
        try {
            this.client.create(this.path, this.data, this.acl, this.createMode);
            children_create=true;
            this.client.create(this.path + "/new_children", this.data, this.acl, this.createMode);
        }catch(KeeperException.NodeExistsException e){
            if(!children_create){
                try {
                    this.client.create(this.path + "/new_children", this.data, this.acl, this.createMode);
                }catch(KeeperException.NodeExistsException e1){
                    //
                }
            }
            else{
                this.version = this.client.exists(this.path+"/new_children",false).getVersion();
            }
        }

        this.client.delete(this.path,this.version);
    }

    @Test
    public void testDelete() throws  InterruptedException, KeeperException{
        Assume.assumeTrue(type == Type.DELETE);
        try{
            this.client.create(this.path,this.data,this.acl,this.createMode);
        }catch(KeeperException.NodeExistsException e){
            this.version = this.client.exists(this.path,false).getVersion();
        }
        this.client.delete(this.path,this.version);
        Stat nodeStat = this.client.exists(this.path,false);
        Assert.assertTrue(nodeStat==null);
    }

    @AfterClass
    public static void removeServer(){
        server.shutdown();
    }
}
