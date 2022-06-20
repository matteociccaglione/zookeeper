package org.apache.zookeeper.client;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ZookeeperACLTest {
    private String path;
    private boolean watch;
    private AddWatchMode mode;
    private byte[] auth;
    private ZooKeeper client;
    private static ZooKeeperServer server;
    private Type type;
    enum Type{
        GET_ACL,
        GET_ACL_NONODE,
        GET_CHILDREN,
        ADD_AUTH,
        ADD_WATCH
    }
    @BeforeClass
    public static void createServer() throws IOException, InterruptedException {
        try {
            int tickTime = 2000;
            int numConnections = 5000;
            String dataDirectory = System.getProperty("java.io.tmpdir");

            File dir = new File(dataDirectory, "zookeeper5").getAbsoluteFile();
            ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
            ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(12346, numConnections);
            int zkPort = standaloneServerFactory.getLocalPort();
            standaloneServerFactory.startup(server);
            ZookeeperACLTest.server = server;
        }catch(Exception e){

        }
    }
    public ZookeeperACLTest(String path, boolean watch, AddWatchMode mode, byte[] auth) throws IOException {
        String connection = "127.0.0.1:12346";
        System.out.println(connection);
        this.client = new ZooKeeper(connection, 2000, event -> {
            //do something with the event processed
        });
        this.path = path;
        this.watch = watch;
        this.mode = mode;
        this.auth = auth;
    }

    @Parameterized.Parameters
    public Collection configure(){
        byte[] data = {};
        return Arrays.asList(new Object[][]{

        });
    }

    @Test
    public void testGetACL() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_ACL);
        byte[] data = {};
        this.client.create(this.path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List< ACL> acls = this.client.getACL(this.path,null);
        boolean areEquals = acls.size()==ZooDefs.Ids.OPEN_ACL_UNSAFE.size();
        for (int i = 0; i < acls.size(); i++){
            if(!acls.get(i).equals(ZooDefs.Ids.OPEN_ACL_UNSAFE.get(i))){
                areEquals = false;
                break;
            }
        }
        Assert.assertTrue(areEquals);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testGetACLNoNode() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_ACL_NONODE);
        this.client.getACL(this.path,null);
    }



    @AfterClass
    public static void removeServer(){
        server.shutdown();
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, "zookeeper5").getAbsoluteFile();
        //dir.delete();
    }
}
