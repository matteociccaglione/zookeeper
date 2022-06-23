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
import java.nio.charset.StandardCharsets;
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
    private ServerCnxnFactory factory;
    private int portIncrement = 0;
    enum Type{
        EXISTS,
        GET_DATA,
        GET_DATA_NONODE,
        SET_DATA,
        SET_DATA_NONODE,
        SET_DATA_BADVER,
        EXISTS_ILLEGAL,
        SET_DATA_BADARG
    }
    private static void cleanDirectory(File dir){
        File[] files = dir.listFiles();
        if(files==null){
            return;
        }
        if(files.length!=0){
            for (File file: files){
                if(file.isFile()){
                    file.delete();
                }
                else if (file.isDirectory()){
                    cleanDirectory(file);
                }
            }
        }
        dir.delete();
    }

    @Before
    public void createServer() throws IOException, InterruptedException {
            this.client=null;
            System.out.println("Before method");
            int tickTime = 2000;
            int numConnections = 5000;
            String dataDirectory = System.getProperty("java.io.tmpdir");

            File dir = new File("/home/utente/Scrivania", "zookeeper19").getAbsoluteFile();
            cleanDirectory(dir);
            ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
            ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(12367, numConnections);
            System.out.println("Factory created");
            int zkPort = standaloneServerFactory.getLocalPort();
            standaloneServerFactory.startup(server);
            System.out.println("Server startup");
            ZookeeperDataTest.server = server;
            this.factory = standaloneServerFactory;
            String connection = "127.0.0.1:"+String.valueOf(12367);
            portIncrement+=1;
            System.out.println(connection);
                this.client = new ZooKeeper(connection, 2000, event -> {
                    //do something with the event processed
                });

    }
    public ZookeeperDataTest(String path, boolean watch, Stat stat, byte[] data, int version, Type type) throws IOException, InterruptedException {

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
                {"/",false,null,"1010".getBytes(StandardCharsets.UTF_8),-1,Type.EXISTS},

                {"/test_exists",false,null,"1010".getBytes(StandardCharsets.UTF_8),-1,Type.EXISTS},
                {"/test_exists2",true,null,"1010".getBytes(StandardCharsets.UTF_8),-1,Type.EXISTS},
                {"/test_illegal",true,null,data,-1,Type.EXISTS_ILLEGAL},
                {"/test_setdata",false,null,"1234".getBytes(StandardCharsets.UTF_8),-1,Type.SET_DATA},
                {"/test_setdata2",false,null,data,0,Type.SET_DATA},
                {"/test_setdata_bad",false,null,data,0,Type.SET_DATA_BADVER},
                {"/test_setdata_nonod",false,null,"101010".getBytes(StandardCharsets.UTF_8),0,Type.SET_DATA_NONODE},
                {"/test_getdata",false,new Stat(),"100".getBytes(StandardCharsets.UTF_8),0,Type.GET_DATA},
                {"/test_getdata2",true,null,"1010".getBytes(StandardCharsets.UTF_8),0,Type.GET_DATA},
                {"/",true,new Stat(),"1010".getBytes(StandardCharsets.UTF_8),0,Type.SET_DATA},
                {"/test_getdata_nonod",false,null,data,0,Type.GET_DATA_NONODE}


        });
    }

    @Test(expected = KeeperException.BadArgumentsException.class)
    public void testSetDataBadArg() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.SET_DATA_BADARG);
        this.client.setData(this.path, this.data, this.version);
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
        Assert.assertEquals(stat.getDataLength(), data.length);
    }

    @Test(expected = IllegalStateException.class)
    public void testExistsIllegal() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.EXISTS_ILLEGAL);

        byte[] data = {};
        this.client.register(null);
        this.client.exists(this.path,true);
    }

    @Test
    public void testSetData1() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA);

        byte[] oldData = {};
        try {
            this.client.create(this.path, oldData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e ){
            this.version = this.client.exists(this.path,false).getVersion();
        }
        Stat stat = this.client.setData(this.path,this.data,this.version);
        Assert.assertEquals(stat.getDataLength(),this.data.length);
    }

    @Test
    public void testSetData2() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA || type == Type.GET_DATA);

        byte[] oldData = {};
        try {
            this.client.create(this.path, oldData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){
            this.version = this.client.exists(this.path,false).getVersion();
        }
        Stat stat = this.client.setData(this.path,this.data,this.version);
        byte[] newData = this.client.getData(this.path,false,stat);
        boolean areEquals = this.data.length==newData.length;
        for(int i = 0; i < this.data.length; i++){
            if(newData[i]!=this.data[i]){
                areEquals = false;
                break;
            }
        }
        Assert.assertTrue(areEquals);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void testSetDataNoNode() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA_NONODE);

        this.client.setData(this.path,this.data,this.version);
    }

    @Test(expected = KeeperException.BadVersionException.class)
    public void testSetDataBadVer() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_DATA_BADVER);

        try {
            this.client.create(this.path, this.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){
            //
        }
        int ver = this.client.exists(this.path,false).getVersion();
        this.client.setData(this.path,this.data,ver+1);
    }

    @Test
    public void getData() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_DATA);


            try {
                this.client.create(this.path, this.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                int ver = this.client.exists(this.path, false).getVersion();
                this.client.delete(this.path, ver);
                this.client.create(this.path, this.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            byte[] newData = this.client.getData(this.path, this.watch, this.stat);
            boolean areEquals = this.data.length == newData.length;
            for (int i = 0; i < this.data.length; i++) {
                if (newData[i] != this.data[i]) {
                    areEquals = false;
                    break;
                }
            }
            Assert.assertTrue(areEquals);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void getDataNoNodeEx() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_DATA_NONODE);

        this.client.getData(this.path,this.watch,this.stat);
    }

    @After
    public void removeServer(){
        System.out.println("After method");
        try {
            this.client.close();
            this.factory.shutdown();
            String dataDirectory = System.getProperty("java.io.tmpdir");

            File dir = new File("/home/utente/Scrivania", "zookeeper19").getAbsoluteFile();
            dir.delete();
        }catch(Exception e){

        }
    }
}
