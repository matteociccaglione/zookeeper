package org.apache.zookeeper.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ZookeeperDataTest {
    private String path;
    private boolean watch;
    private Stat stat;
    private byte[] data;
    private int version;
    private Type type;
    private static ZooKeeperServer server;
    private ZooKeeper client;
    enum Type{
        EXISTS,
        GET_DATA,
        GET_DATA_NONODE,
        SET_DATA,
        EXISTS_ILLEGAL
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
            standaloneServerFactory.startup(server);
            ZookeeperDataTest.server = server;
        }catch(Exception e){

        }
    }
    public ZookeeperDataTest(String path, boolean watch, Stat stat, byte[] data, int version, Type type) throws IOException {
        String connection = "127.0.0.1:12345";
        System.out.println(connection);
        this.client = new ZooKeeper(connection, 2000, event -> {
            //do something with the event processed
        });
        this.path = path;
        this.watch = watch;
        this.stat = stat;
        this.data = data;
        this.version = version;
        this.type=type;
    }

    @Parameterized.Parameters
    public static Collection configure() {
        byte[] data = {};
        return Arrays.asList(new Object[][]{

        });
    }

    @Test
    public void testExists()throws KeeperException,InterruptedException{
        Assume.assumeTrue(type==Type.EXISTS);
        byte[] data = {};
        try{
            this.client.create(this.path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){
        }
        Stat stat = this.client.exists(this.path,this.watch);
        Assert.assertTrue(stat.getDataLength()==data.length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExistsIllegal() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.EXISTS_ILLEGAL);
        byte[] data = {};
        this.client.register(null);
        this.client.exists(this.path,true);
    }



    @AfterClass
    public static void removeServer(){
        server.shutdown();
    }
}
