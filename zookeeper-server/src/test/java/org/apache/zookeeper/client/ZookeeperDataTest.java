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
public class ZookeeperDataTest extends ZookeeperTestBaseClass{
    private String path;
    private boolean watch;
    private Stat stat;
    private byte[] data;
    private int version;
    private Type type;
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

    public ZookeeperDataTest(String path, boolean watch, Stat stat, byte[] data, int version, Type type) throws IOException, InterruptedException {
        this.directoryName = "zookeeper19";
        this.portNumber = 12367;
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
    public void testGetData() throws InterruptedException, KeeperException {
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

}
