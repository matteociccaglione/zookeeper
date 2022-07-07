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
public class ZookeeperTest extends ZookeeperTestBaseClass{
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private CreateMode createMode;
    private Type type;
    private int version;
    private enum Type{
        CREATE_KEEPEREX,
        CREATE,
        CREATE_ILLEGALARGEX,
        CREATE_NODEEXISTS,
        CREATE_KEEPEREF,
        CREATE_BADACL,
        DELETE_KEEPERNONODE,
        DELETE_KEEPERBADVER,
        DELETE_KEEPERNOEMPTY,
        DELETE,
        GET_EPHEMERALS,
        GET_EPHEMERALS_PREFIX
    }
    public ZookeeperTest(String path, byte[] data, ArrayList<ACL> acl, CreateMode createMode,Type type, int version) throws IOException {
        this.directoryName = "zookeeper8";
        this.portNumber = 12349;
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
                {"/test3","2010".getBytes(StandardCharsets.UTF_8),new ArrayList<ACL>(),CreateMode.EPHEMERAL,Type.CREATE_BADACL,0},
                {"/test3","2010".getBytes(StandardCharsets.UTF_8),null,CreateMode.EPHEMERAL,Type.CREATE_BADACL,0},
                {"/test3",data,new ArrayList<ACL>(),CreateMode.EPHEMERAL,Type.CREATE_BADACL,0},
                {"/","2010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, Type.CREATE_NODEEXISTS,0},

                {"/test3",data,null,CreateMode.EPHEMERAL,Type.CREATE_BADACL,0},
                {"/test2",data,ZooDefs.Ids.READ_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL,Type.CREATE,0},
                {"/testfail/no/parent",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_KEEPEREX,0},
                {"/testfail/no2/parent","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_KEEPEREX,0},
                {"\\kestfail/\\ladpath",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_ILLEGALARGEX,0},
                {"\\kestfail/\\ladpath","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_ILLEGALARGEX,0},
                {"/provaeph",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_KEEPEREF,0},
                {"/provaeph4","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_KEEPEREF,0},
                {"/test_no_node",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERNONODE,0},
                {"/test_bad_version",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERBADVER,2},
                {"/test_bad_version2",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERBADVER,-2},
                {"/test_delete",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE,0},
                {"/test_delete_children",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.DELETE_KEEPERNOEMPTY,0},
               {"",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_ILLEGALARGEX,0},
               {"","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.CREATE_ILLEGALARGEX,0},
                {"","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.GET_EPHEMERALS,0},
                {"/","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.GET_EPHEMERALS_PREFIX,0},
                {"/test_eph_from_prefix","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,Type.GET_EPHEMERALS_PREFIX,0}



        });
    }
    @Test(expected = KeeperException.NodeExistsException.class)
    public void testCreateNodeExists() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.CREATE_NODEEXISTS);
        this.client.create(this.path,this.data,this.acl,this.createMode);
    }
    @Test(expected = KeeperException.InvalidACLException.class)
    public void testCreateBadACL() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.CREATE_BADACL);
        this.client.create(this.path,this.data,this.acl,this.createMode);
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
        System.out.println(this.path+String.valueOf(this.data.length));
        String pathReturned = this.client.create(this.path,this.data, this.acl,this.createMode);
    }
    @Test(expected = KeeperException.NoChildrenForEphemeralsException.class)
    public void testCreateKeeperExEphemeral() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.CREATE_KEEPEREF);
        byte[] parentData = {};
        String pathParent;
        try {
            pathParent = this.client.create("/ephemeral_parent1", parentData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }catch(KeeperException.NodeExistsException e){
            pathParent = "/ephemeral_parent1";
        }
        this.client.create(pathParent+this.path,this.data,this.acl, this.createMode);
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
        Assert.assertNull(nodeStat);
    }


    @Test
    public void getEphemerals() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_EPHEMERALS);
        List<String> ephemerals = new ArrayList<>();
        ephemerals.add("/eph1");
        ephemerals.add("/eph2");
        for (String name : ephemerals){
            this.client.create(name,"test".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);

        }
        List<String> returned = this.client.getEphemerals();
        boolean isCorrect = true;
        for (String name: ephemerals){
            if (!returned.contains(name)){
                isCorrect=false;
                break;
            }
        }
        Assert.assertTrue(isCorrect);
    }

    @Test
    public void getEphemeralsPrefix() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.GET_EPHEMERALS_PREFIX);
        boolean wasRoot=false;
        if(!this.path.equals("/")){
            this.client.create(this.path,"test".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
        else{
            this.path="";
            wasRoot=true;
        }
        String badEph = "/eph3";
        this.client.create(this.path+"/eph1","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        this.client.create(this.path+"/eph2","1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        if(this.path.equals(badEph)){
            badEph = "/eph4";
        }
        this.client.create(badEph,"1010".getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        List<String> names = new ArrayList<>();
        names.add(this.path+"/eph1");
        names.add(this.path+"/eph2");
        names.add(badEph);
        if(wasRoot){
            this.path="/";

        }
        List<String> returned = this.client.getEphemerals(this.path);
        boolean isCorrect = returned.contains(names.get(0)) && returned.contains(names.get(1));
        if(!wasRoot){
            isCorrect = isCorrect && !returned.contains(badEph);
        }
        else{
            isCorrect = isCorrect && returned.contains(badEph);
        }
        Assert.assertTrue(isCorrect);
    }

}
