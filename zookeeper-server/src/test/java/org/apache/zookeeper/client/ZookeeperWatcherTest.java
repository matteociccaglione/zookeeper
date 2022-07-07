package org.apache.zookeeper.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.watch.WatcherMode;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.*;


@RunWith(Parameterized.class)
public class ZookeeperWatcherTest extends ZookeeperTestBaseClass{
    private String basePath;
    private AddWatchMode mode;
    private MyWatcher watcher;
    private Watcher.WatcherType watcherType;
    private Type type;
    private boolean local;
    enum Type{
        ADD_WATCH,
        ADD_WATCH_WITH_WATCHER,
        ADD_WATCH_NULL,
        REMOVE_WATCHER,
        REMOVE_WATCHER_EX,

        REMOVE_ALL_WATCHER,
        REMOVE_ALL_WATCHER_EX
    }
    public ZookeeperWatcherTest(String basePath, AddWatchMode mode, Type type, Watcher.WatcherType watcherType,boolean local){
        this.directoryName="zookeeperWatcher";
        this.portNumber=23456;
        this.basePath=basePath;
        this.mode = mode;
        this.type = type;
        this.watcherType=watcherType;
        this.watcher=new MyWatcher();
        this.local = local;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {"/testAdd",AddWatchMode.PERSISTENT,Type.ADD_WATCH,Watcher.WatcherType.Any,false},
                {"/testAdd",AddWatchMode.PERSISTENT_RECURSIVE,Type.ADD_WATCH,Watcher.WatcherType.Any,false},
                {"/testAdd",AddWatchMode.PERSISTENT,Type.ADD_WATCH_WITH_WATCHER,Watcher.WatcherType.Any,false},
                {"/testAdd",AddWatchMode.PERSISTENT_RECURSIVE,Type.ADD_WATCH_WITH_WATCHER,Watcher.WatcherType.Any,false},
                {"/testAddNull",AddWatchMode.PERSISTENT,Type.ADD_WATCH_NULL,Watcher.WatcherType.Any,false},
                {"/testRemove",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER,Watcher.WatcherType.Any,false},
                {"/testRemove",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER,Watcher.WatcherType.Any,true},
                {"/testRemove",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER,Watcher.WatcherType.Data,false},
                {"/testRemove",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER,Watcher.WatcherType.Data,true},
                {"/testRemove",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER,Watcher.WatcherType.Children,false},
                {"/testRemove",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER,Watcher.WatcherType.Children,true},
                {"/testRemoveNoWatch",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER_EX,Watcher.WatcherType.Any,false},
                {"/testRemoveNoWatch",AddWatchMode.PERSISTENT,Type.REMOVE_WATCHER_EX,Watcher.WatcherType.Any,true},
                {"/testRemoveWatches",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER,Watcher.WatcherType.Any,false},
                {"/testRemoveWatches",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER,Watcher.WatcherType.Any,true},
                {"/testRemoveWatches",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER,Watcher.WatcherType.Data,false},
                {"/testRemoveWatches",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER,Watcher.WatcherType.Data,true},
                {"/testRemoveWatches",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER,Watcher.WatcherType.Children,false},
                {"/testRemoveWatches",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER,Watcher.WatcherType.Children,true},
                {"/testRemoveNoWatch",AddWatchMode.PERSISTENT,Type.REMOVE_ALL_WATCHER_EX,Watcher.WatcherType.Any,true},


        });
    }

    @Test
    public void testAddWatch() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.ADD_WATCH);
        byte[] data = {};
        watcher = new MyWatcher();
        this.client.register(watcher);
        this.client.create(this.basePath,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        this.client.addWatch(this.basePath,this.mode);
        this.client.setData(this.basePath,"newdata".getBytes(StandardCharsets.UTF_8),0);
        WatchedEvent event = watcher.events.get(watcher.events.size()-1);
        Assert.assertTrue(new EventComparison().isCorrect(event));
    }

    @Test
    public void testAddWatchWithWatcher() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.ADD_WATCH_WITH_WATCHER);
        byte[] data = {};
        this.client.create(this.basePath,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        this.client.addWatch(this.basePath,this.watcher,this.mode);
        this.client.setData(this.basePath,"newdata".getBytes(StandardCharsets.UTF_8),0);
        WatchedEvent event = watcher.events.get(watcher.events.size()-1);
        Assert.assertTrue(new EventComparison().isCorrect(event));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddWatchNull() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type == Type.ADD_WATCH_NULL);
        byte[] data = {};
        this.watcher=null;
        this.client.create(this.basePath,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        this.client.addWatch(this.basePath,this.watcher,this.mode);
    }


    @Test
    public void testRemoveWatcher() throws InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.REMOVE_WATCHER);
        byte[] data = {};
        boolean isCorrect=true;
        this.client.create(this.basePath,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        this.watcher=new WatcherToRemove();
        if(this.watcherType== Watcher.WatcherType.Any){
            this.client.addWatch(this.basePath,watcher, AddWatchMode.PERSISTENT);
            this.client.removeWatches(this.basePath,this.watcher,this.watcherType,this.local);
            this.client.setData(this.basePath,"newdata".getBytes(StandardCharsets.UTF_8),0);
            isCorrect = this.watcher.events.isEmpty();
        }
        if(this.watcherType == Watcher.WatcherType.Data){
            Stat stat=new Stat();
            this.client.getData(this.basePath,this.watcher,stat);
            this.client.removeWatches(this.basePath,this.watcher,this.watcherType,this.local);
            this.client.setData(this.basePath,"newdata".getBytes(StandardCharsets.UTF_8),0);
            isCorrect = this.watcher.events.isEmpty();
        }
        if(this.watcherType == Watcher.WatcherType.Children){
            Stat stat = new Stat();
            this.client.getChildren(this.basePath,this.watcher,stat);
            this.client.removeWatches(this.basePath,this.watcher,this.watcherType,this.local);
            this.client.setData(this.basePath,"newdata".getBytes(StandardCharsets.UTF_8),0);
            isCorrect = this.watcher.events.isEmpty();
        }
        Assert.assertTrue(isCorrect);
    }

    @Test(expected = KeeperException.NoWatcherException.class)
    public void testRemoveNoWatcher() throws InterruptedException, KeeperException {
        byte[] data={};
        Assume.assumeTrue(this.type==Type.REMOVE_WATCHER_EX || this.type==Type.REMOVE_ALL_WATCHER_EX);
        this.client.create(this.basePath,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        if(this.type==Type.REMOVE_WATCHER_EX){
            this.client.removeWatches(this.basePath,this.watcher,this.watcherType,this.local);
        }
        else{
            this.client.removeAllWatches(this.basePath,this.watcherType,this.local);
        }
    }


    @Test
    public void testRemoveAllWatches() throws InterruptedException, KeeperException {
        Assume.assumeTrue(this.type == Type.REMOVE_ALL_WATCHER);
        byte[] data={};
        this.client.create(this.basePath,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        MyWatcher watcher1 = new WatcherToRemove();
        MyWatcher watcher2 = new WatcherToRemove();
        boolean isCorrect = true;
        if(this.watcherType== Watcher.WatcherType.Any) {
            this.client.addWatch(this.basePath,watcher1,AddWatchMode.PERSISTENT);
            this.client.addWatch(this.basePath,watcher2,AddWatchMode.PERSISTENT);
            this.client.removeAllWatches(this.basePath,this.watcherType,this.local);
            this.client.setData(this.basePath,"newdata".getBytes(StandardCharsets.UTF_8),0);
            isCorrect = watcher1.events.isEmpty() && watcher2.events.isEmpty();
        }
        if(this.watcherType== Watcher.WatcherType.Data){
            Stat stat = new Stat();
            this.client.getData(this.basePath,watcher1,stat);
            this.client.getData(this.basePath,watcher2,stat);
            this.client.removeAllWatches(this.basePath,this.watcherType,this.local);
            this.client.setData(this.basePath,"newData".getBytes(StandardCharsets.UTF_8),0);
            isCorrect = watcher1.events.isEmpty() && watcher2.events.isEmpty();
        }
        if(this.watcherType==Watcher.WatcherType.Children){
            Stat stat = new Stat();
            this.client.getChildren(this.basePath,watcher1,stat);
            this.client.getChildren(this.basePath,watcher2,stat);
            this.client.removeAllWatches(this.basePath,this.watcherType,this.local);
            this.client.setData(this.basePath,"newData".getBytes(StandardCharsets.UTF_8),0);
            isCorrect = watcher1.events.isEmpty() && watcher2.events.isEmpty();
        }
        Assert.assertTrue(isCorrect);
    }

    private class EventComparison{
        public  boolean isCorrect(WatchedEvent event){
            return event.getType().equals(Watcher.Event.EventType.NodeDataChanged) && Objects.equals(event.getPath(), basePath);
        }
    }

    private class MyWatcher implements Watcher{
        public List<WatchedEvent> events = new ArrayList<>();

        @Override
        public void process(WatchedEvent event) {
            if(event.getType()!= Event.EventType.None) {
                events.add(event);
            }
        }
    }
    private class WatcherToRemove extends MyWatcher{

        @Override
        public void process(WatchedEvent event) {
            if(event.getType().equals(Event.EventType.NodeDataChanged)){
                this.events.add(event);
            }
        }
    }
}
