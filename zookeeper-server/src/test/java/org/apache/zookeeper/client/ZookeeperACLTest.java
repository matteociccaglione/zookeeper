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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ZookeeperACLTest extends ZookeeperTestBaseClass{
    private String path;
    private boolean watch;
    private List<ACL> acl;
    private int aclVersion;
    private Type type;
    enum Type{
        GET_ACL,
        GET_ACL_NONODE,
        GET_CHILDREN,
        GET_CHILDREN_NONODE,
        SET_ACL,
        SET_ACL_NONODE,
        SET_ACL_BADVER
    }

    public ZookeeperACLTest(String path, boolean watch, ArrayList<ACL> acl, int aclVersion, Type type) throws IOException {
        this.directoryName = "zookeeper15";
        this.portNumber = 12347;
        this.path = path;
        this.watch = watch;
        this.acl = acl;
        this.aclVersion = aclVersion;
        this.type = type;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        byte[] data = {};
        List<ACL> l = new ArrayList<>();
        return Arrays.asList(new Object[][]{
                {"/test_get_acl",true,ZooDefs.Ids.OPEN_ACL_UNSAFE,-1,Type.GET_ACL},
                {"/test_get_acl2",false,ZooDefs.Ids.READ_ACL_UNSAFE,0,Type.GET_ACL},
                {"/test_get_acl_nonode",true,ZooDefs.Ids.OPEN_ACL_UNSAFE,-1,Type.GET_ACL_NONODE},
                {"/test_parent_children",true,ZooDefs.Ids.OPEN_ACL_UNSAFE,-1,Type.GET_CHILDREN},
                {"/test_parent_children2",false,ZooDefs.Ids.OPEN_ACL_UNSAFE,-1,Type.GET_CHILDREN},
                {"/test_set_acl",true,ZooDefs.Ids.READ_ACL_UNSAFE,-1,Type.SET_ACL},
                //{"/test_no_node_set_acl",false,ZooDefs.Ids.OPEN_ACL_UNSAFE,Type.SET_ACL_NONODE},
                {"/test_set_acl_badver",false,ZooDefs.Ids.OPEN_ACL_UNSAFE,10,Type.SET_ACL_BADVER}
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

    @Test(expected = KeeperException.NoNodeException.class)
    public void testGetChildrenNoNode() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_CHILDREN_NONODE);
        this.client.getChildren(this.path,this.watch);
    }

    @Test
    public void testGetChildren() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_CHILDREN);
        byte[] data = {};
        try {
            this.client.create(this.path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){

        }
        this.client.create(this.path+"/child1",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        this.client.create(this.path+"/child2",data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        List<String> childs = this.client.getChildren(this.path,this.watch);
        for (String child: childs){
            System.out.println(child);
        }
        boolean isCorrect = childs.contains("child1") && childs.contains("child2");
        Assert.assertTrue(isCorrect);
    }
/*
    @Test
    public void testGetChildren2() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.GET_CHILDREN);
        byte[] data = {};
        try {
            this.client.create(this.path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){

        }
        List<String> childs = this.client.getChildren(this.path,this.watch);
        Assert.assertTrue(childs.isEmpty());
    }
*/
    @Test
    public void testSetAcl() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.SET_ACL || type==Type.GET_ACL);
        byte[] data = {};
        try{
            this.client.create(this.path,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){
            this.aclVersion = this.client.exists(this.path,false).getAversion();
        }
        this.client.setACL(this.path,this.acl,this.aclVersion);
        List<ACL> acls = this.client.getACL(this.path,null);
        boolean areEquals = acls.size()==this.acl.size();
        for (int i = 0; i<acls.size(); i++){
            if(!this.acl.contains(acls.get(i))){
                areEquals = false;
                break;
            }
        }
        Assert.assertTrue(areEquals);
    }
    @Test(expected = KeeperException.NoNodeException.class)
    public void testSetAclNoNode() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_ACL_NONODE);
        this.client.setACL(this.path,this.acl,this.aclVersion);
    }

    @Test(expected = KeeperException.BadVersionException.class)
    public void testSetAclBadVersion() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.SET_ACL_BADVER);
        byte[] data = {};
        try {
            this.client.create(this.path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException.NodeExistsException e){
            this.aclVersion = this.client.exists(this.path,false).getAversion()+1;
        }
        this.client.setACL(this.path,this.acl,this.aclVersion);
    }

}
